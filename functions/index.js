import { initializeApp } from 'firebase-admin/app';
import { getFirestore, FieldValue, Timestamp, GeoPoint } from 'firebase-admin/firestore';
import { getMessaging } from 'firebase-admin/messaging';
import { getStorage } from 'firebase-admin/storage';

import * as functions from 'firebase-functions';

import { onRequest } from 'firebase-functions/v2/https';
// import { onUserCreated } from 'firebase-functions/v2/identity';
import { onDocumentUpdated, onDocumentCreated, onDocumentWritten } from 'firebase-functions/v2/firestore';
import { onObjectFinalized } from 'firebase-functions/v2/storage';
import { onSchedule } from 'firebase-functions/v2/scheduler';

import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import path from 'path';
import csv from 'csv-parser';
import fs from 'fs-extra';
import os from 'os';
import crypto from 'crypto';
import * as geofire from 'geofire-common';
import pLimit from 'p-limit';

const app = initializeApp();
const db = getFirestore(app);
const messaging = getMessaging(app);
const storage = getStorage(app);
const bucket = storage.bucket();

/*********************************************************
 * Geolocation-Grouped Weather — config & feature flag
 * See devnotes/geolocation-grouping-weather-plan-2026-07-17.md
 *********************************************************/

// Master switch for the group-based weather path. When true:
//  - legacy checkWeatherStatusPubSub is skipped (group path owns weather checks)
//  - inletStatusUpdatedV2's job/notification body no-ops (group path owns jobs/pushes)
//  - the scheduled checkGroupWeather runs fully (weather + jobs + notifications)
// When false, everything legacy runs and the group scheduled fns idle; the manual
// triggers (triggerRebuildGroups / triggerGroupWeather) still work for backfill/shadow.
const GROUP_WEATHER_ENABLED = true;

// Weather is effectively homogeneous within an NWS grid cell (~2.5 km). 5 km keeps
// each group inside a single weather regime while minimizing group count.
export const GROUP_RADIUS_METERS = 5000;
export const GROUP_WEATHER_CONCURRENCY = 5; // parallel NWS pings across groups
export const RISK_THRESHOLD = 35; // matches legacy inletStatusUpdatedV2
export const JOB_DEBOUNCE_MS = 48 * 60 * 60 * 1000;
// A rebuilt group whose center lands within this distance of an existing group's
// center reuses that group doc (carrying forward resolved NWS URLs + debounce state),
// keeping group identity stable day-to-day. 500 m << the 2.5 km NWS cell, so a match
// is guaranteed to be the same weather cell.
const GROUP_CENTER_MATCH_METERS = 500;

// Commit an array of items across Firestore batches, chunked under the 500-write cap.
async function runBatched(items, applyFn, chunkSize = 500) {
  for (let i = 0; i < items.length; i += chunkSize) {
    const batch = db.batch();
    for (const item of items.slice(i, i + chunkSize)) {
      applyFn(batch, item);
    }
    await batch.commit();
  }
}

/*********************************************************
 * User Document Creation on Auth Signup
 *********************************************************/
export const createUserDoc = functions.auth.user().onCreate((user) => {
  const { uid, email, displayName, photoURL } = user;

  return db.collection('users').doc(uid).set({
    email,
    displayName,
    photoURL,
    createdAt: FieldValue.serverTimestamp(),
  });
});

/*********************************************************
 * PubSub: Weather Check
 *********************************************************/
export const checkWeatherStatusPubSub = onSchedule(
  {
    // Hourly is plenty — NWS 48h forecasts don't change minute-to-minute, and
    // the previous every-1-minute cadence made ~3 external NWS calls per ready
    // inlet per minute (thousands/min at scale), blowing past NWS fair-use.
    schedule: '0 * * * *',
    timeZone: 'America/New_York',
    region: 'us-east4',
    nodeVersion: '20',
    timeoutSeconds: 540,
    memory: '512MiB',
  },
  async () => {
    if (GROUP_WEATHER_ENABLED) {
      console.log('[checkWeatherStatusPubSub] Skipped: GROUP_WEATHER_ENABLED — the group weather path owns weather checks.');
      return null;
    }
    await checkWeatherStatus();
    return null;
  },
);

const START_HOUR = 7;
const END_HOUR = 22;

const getNextSendTime = (now) => {
  const sendTime = new Date(now);
  const hour = sendTime.getHours();

  if (hour >= START_HOUR && hour <= END_HOUR) {
    sendTime.setMinutes(sendTime.getMinutes() + 5);
    return sendTime;
  }

  if (hour >= END_HOUR) {
    sendTime.setDate(sendTime.getDate() + 1);
  }

  sendTime.setHours(START_HOUR, 5, 0, 0);
  return sendTime;
};

export const schedulePushNotifications = async (db, params) => {
  if (!params.tokens || params.tokens.length === 0) return;

  const now = new Date();
  const sendAtDate = getNextSendTime(now);

  await db.collection('scheduledNotifications').add({
    tokens: params.tokens,
    title: params.title,
    body: params.body,
    sendAt: Timestamp.fromDate(sendAtDate),
    status: 'pending',
    createdAt: Timestamp.fromDate(now),
  });
};

/*****************************************************************
 * Send scheduled notifications
 ****************************************************************/
export const sendScheduledNotifications = onSchedule(
  {
    schedule: 'every 1 minutes',
    region: 'us-east4',
    nodeVersion: '20',
  },
  async () => {
    console.log('[sendScheduledNotifications] Setting up tasks...');

    const now = Timestamp.now();

    const snapshot = await db.collection('scheduledNotifications').where('status', '==', 'pending').where('sendAt', '<=', now).limit(100).get();

    if (snapshot.empty) {
      console.log('[sendScheduledNotifications] No pending notifications found.');
      return;
    }

    for (const doc of snapshot.docs) {
      const data = doc.data();

      try {
        const response = await messaging.sendEachForMulticast({
          tokens: data.tokens,
          notification: {
            title: data.title,
            body: data.body,
          },
          android: { priority: 'high' },
        });

        await doc.ref.update({
          status: 'sent',
          sentAt: Timestamp.now(),
        });
      } catch (e) {
        await doc.ref.update({
          status: 'failed',
          error: e.message,
        });
      }
    }

    console.log('[sendScheduledNotifications] Completed.');
  },
);

/*********************************************************
 * Archive Old Pending Jobs
 *********************************************************/
export const archiveOldPendingJobs = onSchedule(
  {
    schedule: '0 0 * * *',
    timeZone: 'America/New_York',
    region: 'us-east4',
    nodeVersion: '20',
  },
  async () => {
    const twoWeeksAgo = new Date();
    twoWeeksAgo.setDate(twoWeeksAgo.getDate() - 14);

    const snapshot = await db.collection('inletCleaningJobs').where('status', '==', 'pending').where('createdAt', '<', Timestamp.fromDate(twoWeeksAgo)).get();

    if (snapshot.empty) {
      console.log('No old pending cleaning jobs found.');
      return null;
    }

    const batch = db.batch();
    snapshot.docs.forEach((docSnap) => {
      batch.update(docSnap.ref, {
        status: 'archived',
        archivedAt: FieldValue.serverTimestamp(),
      });
    });

    await batch.commit();
    console.log(`Archived ${snapshot.size} pending jobs older than 2 weeks.`);
    return null;
  },
);

/*********************************************************
 * Weather Digest: Re-engagement Notifications
 *********************************************************/
const DIGEST_INACTIVE_DAYS = 14;

async function resolveNwsPoint(lat, lng, cache) {
  const key = `${lat.toFixed(2)},${lng.toFixed(2)}`;
  if (cache.has(key)) return cache.get(key);

  const res = await fetch(`https://api.weather.gov/points/${lat},${lng}`);
  if (!res.ok) throw new Error(`NWS points API ${res.status} for ${lat},${lng}`);

  const json = await res.json();
  const result = {
    forecastUrl: json.properties.forecast,
    gridPointUrl: json.properties.forecastGridData,
    city: json.properties.relativeLocation.properties.city,
    state: json.properties.relativeLocation.properties.state,
  };
  cache.set(key, result);
  return result;
}

async function buildWeatherDigest(forecastUrl, city) {
  const forecastRes = await fetch(forecastUrl);
  if (!forecastRes.ok) throw new Error(`NWS forecast API ${forecastRes.status}`);

  const forecastJson = await forecastRes.json();
  const periods = forecastJson.properties.periods;
  const rainyPeriods = periods.filter((p) => (p.probabilityOfPrecipitation?.value ?? 0) > 30);

  if (rainyPeriods.length > 0) {
    const names = rainyPeriods.slice(0, 3).map((p) => p.name);
    const dayStr = names.length === 1 ? names[0] : names.slice(0, -1).join(', ') + ' & ' + names[names.length - 1];
    return {
      title: `Rain in the ${city} forecast`,
      body: `Rain is expected ${dayStr}. Your local inlets may need attention soon.`,
    };
  }

  return {
    title: `Dry week ahead in ${city}`,
    body: 'No significant rain expected for the next 7 days. A great time to explore Cleanlet!',
  };
}

async function runWeatherDigest() {
  console.log('[sendWeatherDigest] Starting re-engagement digest...');

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - DIGEST_INACTIVE_DAYS);
    const usersSnap = await db.collection('users').where('appLastUsed', '<=', Timestamp.fromDate(cutoff)).get();

    if (usersSnap.empty) {
      console.log('[sendWeatherDigest] No inactive users found, skipping.');
      return;
    }

    const nwsCache = new Map();
    const cityMap = new Map();

    for (const userDoc of usersSnap.docs) {
      const user = userDoc.data();
      if (!user.tokens || user.tokens.length === 0) continue;

      const userId = userDoc.id;
      const inletsSnap = await db.collection('inlets').where('subscribed', 'array-contains', userId).get();
      if (inletsSnap.empty) continue;

      for (const inletDoc of inletsSnap.docs) {
        const { geoLocation } = inletDoc.data();
        if (!geoLocation) continue;

        let nwsPoint;
        try {
          nwsPoint = await resolveNwsPoint(geoLocation.latitude, geoLocation.longitude, nwsCache);
        } catch (err) {
          console.error(`[sendWeatherDigest] ${err.message}`);
          continue;
        }

        const { city, forecastUrl } = nwsPoint;
        if (!cityMap.has(city)) {
          cityMap.set(city, { forecastUrl, userTokens: new Map() });
        }
        cityMap.get(city).userTokens.set(userId, user.tokens);
      }
    }

    if (cityMap.size === 0) {
      console.log('[sendWeatherDigest] No city data resolved, skipping.');
      return;
    }

    for (const [city, { forecastUrl, userTokens }] of cityMap) {
      let digest;
      try {
        digest = await buildWeatherDigest(forecastUrl, city);
      } catch (err) {
        console.error(`[sendWeatherDigest] Failed to build digest for ${city}: ${err.message}`);
        continue;
      }

      const tokens = [...userTokens.values()].flat();
      await schedulePushNotifications(db, { tokens, title: digest.title, body: digest.body });
      console.log(`[sendWeatherDigest] Scheduled ${city} digest for ${tokens.length} tokens.`);
    }

    console.log(`[sendWeatherDigest] Done. Processed ${cityMap.size} city/cities.`);
  } catch (err) {
    console.error(`[sendWeatherDigest] Fatal error: ${err.message}`);
  }
}

export const sendWeatherDigest = onSchedule(
  {
    schedule: '0 8 * * *',
    timeZone: 'America/New_York',
    region: 'us-east4',
    nodeVersion: '20',
  },
  async () => {
    await runWeatherDigest();
    return null;
  },
);

export const triggerWeatherDigest = onRequest(
  {
    region: 'us-east4',
    nodeVersion: '20',
  },
  async (_req, res) => {
    await runWeatherDigest();
    res.send('Weather digest triggered.');
  },
);

/*********************************************************
 * Manual Weather Trigger
 *********************************************************/
export const triggerWeatherStatus = onRequest(
  {
    region: 'us-east4',
    nodeVersion: '20',
  },
  async (req, res) => {
    await checkWeatherStatus();
    res.send('Triggered');
  },
);

// export const testAdminNotification = onRequest(
//   {
//     region: 'us-east4',
//     nodeVersion: '20',
//   },
//   async (req, res) => {
//     const userDocs = await db.collection('users').where('role', '==', 'admin').get();

//     if (!userDocs.empty) {
//       for (const userDoc of userDocs.docs) {
//         const user = userDoc.data();
//         if (user.tokens) {
//           const message = {
//             tokens: user.tokens,
//             notification: {
//               title: 'Test Admin Notification',
//               body: 'This is a test admin notification.',
//             },
//             android: { priority: 'high' },
//           };

//           const response = await messaging.sendEachForMulticast(message);

//           response.responses.forEach((r, i) => {
//             if (r.success) console.log(`Message to ${user.tokens[i]} succeeded`);
//             else console.error(`Message failed: ${r.error?.message}`);
//           });
//         }
//       }
//     } else {
//       console.log('No admins found.');
//     }

//     res.status(200).send('Test complete');
//   },
// );

/*********************************************************
 * Cleaning Job Status Updated
 *********************************************************/
export const cleaningJobStatusUpdatedV2 = onDocumentUpdated(
  {
    document: 'inletCleaningJobs/{inletCleaningJobId}',
    region: 'us-east4',
    nodeVersion: '20',
  },
  async (event) => {
    const newValue = event.data.after.data();
    const oldValue = event.data.before.data();

    let tokens = [];

    if (newValue.status === 'completed') {
      const userDocs = await db.collection('users').where('role', '==', 'admin').get();

      if (!userDocs.empty) {
        for (const userDoc of userDocs.docs) {
          const user = userDoc.data();
          if (user.tokens) {
            tokens.push(...user.tokens);
          }
        }

        if (tokens.length > 0) {
          const message = {
            tokens,
            notification: {
              title: 'A cleaning job has been completed',
              body: 'A recent cleaning job has been completed by a volunteer. Please review in admin panel.',
            },
            android: { priority: 'high' },
          };

          const response = await messaging.sendEachForMulticast(message);

          response.responses.forEach((r, i) => {
            if (r.success) console.log(`Message to ${tokens[i]} succeeded`);
            else console.error(`Message failed: ${r.error?.message}`);
          });
        }
      }
    }
  },
);

/*********************************************************
 * Firestore Listener: Inlet Status Updated
 *********************************************************/
export const inletStatusUpdatedV2 = onDocumentUpdated(
  {
    document: 'inlets/{inletId}',
    region: 'us-east4',
    nodeVersion: '20',
  },
  async (event) => {
    // Phase 1 of retiring this per-inlet trigger: when the group weather path is
    // live it owns job creation + notifications, so this body must no-op. Otherwise
    // checkGroupWeather's batched weather fan-out would re-fire a job/push per inlet.
    if (GROUP_WEATHER_ENABLED) {
      console.log(`[inletStatusUpdatedV2] No-op for ${event.params.inletId}: GROUP_WEATHER_ENABLED — checkGroupWeather owns jobs/notifications.`);
      return;
    }

    const newValue = event.data.after.data();
    const oldValue = event.data.before.data();

    if (!newValue || !oldValue) return;

    const lastNotification = newValue.lastNotificationAndCleaningJobCreated;
    const now = Timestamp.now();

    const riskIncreased = oldValue.risk !== newValue.risk && newValue.risk > 35;
    const heavyRainExpected = newValue.heavyRainExpected === true;
    const enoughTimePassed = !lastNotification || now.toMillis() - lastNotification.toMillis() >= 48 * 60 * 60 * 1000;

    if (!heavyRainExpected) {
      console.log(`[Inlet ${event.params.inletId}] Risk changed but rainfall below threshold (${newValue.rainNext48Inches} in.) `);
      return;
    }

    if (!riskIncreased || !enoughTimePassed) return;

    console.log(`[Inlet ${event.params.inletId}] High risk + Heavy rain detected, creating cleaning job...`);

    await createInletCleaningJob(event.params.inletId, newValue.risk);

    // Stamp the 48h debounce timestamp immediately after the job is created,
    // BEFORE gathering tokens / scheduling the push. If anything below fails,
    // the debounce is still recorded so the next weather update can't create a
    // duplicate cleaning job for the same inlet.
    await db.collection('inlets').doc(event.params.inletId).update({ lastNotificationAndCleaningJobCreated: now });

    let tokens = [];

    for (const userId of newValue.subscribed ?? []) {
      const userDoc = await db.collection('users').doc(userId).get();
      if (userDoc.exists && userDoc.data().tokens) {
        tokens.push(...userDoc.data().tokens);
      }
    }

    await schedulePushNotifications(db, {
      tokens,
      title: 'Inlet Cleaning Needed',
      body: oldValue?.address ? `The Inlet at ${oldValue.address} needs cleaning.` : 'An Inlet you follow requires cleaning.',
    });
  },
);

/*********************************************************
 * Storage Trigger: CSV Imports
 *********************************************************/
export const checkUploadedImageV2 = onObjectFinalized(
  {
    region: 'us-east4',
    nodeVersion: '20',
  },
  async (event) => {
    const { bucket: bucketName, name: filePath, contentType } = event.data;

    const fileDir = path.dirname(filePath);
    if (fileDir !== 'inlet-uploads' || !['text/csv', 'application/vnd.ms-excel'].includes(contentType)) {
      console.log('Skipping non-CSV or invalid upload.');
      return null;
    }

    const results = [];
    const bucketRef = getStorage().bucket(bucketName);
    const tempFilePath = path.join(os.tmpdir(), path.basename(filePath));

    await fs.ensureDir(path.dirname(tempFilePath));
    await bucketRef.file(filePath).download({ destination: tempFilePath });

    return new Promise((resolve) => {
      fs.createReadStream(tempFilePath)
        .pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', async () => {
          for (const row of results) {
            const hash = geofire.geohashForLocation([parseFloat(row.latitude), parseFloat(row.longitude)]);

            await db
              .collection('inlets')
              .doc(hash)
              .set(
                {
                  geoHash: hash,
                  geoLocation: new GeoPoint(parseFloat(row.latitude), parseFloat(row.longitude)),
                  address: row.address,
                  description: row.description,
                  images: row.images,
                  instructions: row.instructions,
                },
                { merge: true },
              );
          }
          resolve(null);
        });
    });
  },
);

/*********************************************************
 * Test Push Notifications
 *********************************************************/
export const testPushNotifications = onRequest(
  {
    region: 'us-east4',
    nodeVersion: '20',
  },
  async (req, res) => {
    try {
      const userId = req.query.user;
      const userDoc = await db.collection('users').doc(userId).get();

      const tokens = userDoc.data()?.tokens ?? [];
      if (tokens.length === 0) {
        return res.status(200).send('No push tokens for user.');
      }

      const message = {
        tokens,
        notification: {
          title: 'Cleanlet Test',
          body: 'If you are receiving this message, this is a test.',
        },
        android: { priority: 'high' },
      };

      const response = await messaging.sendEachForMulticast(message);
      console.log('FCM Response:', JSON.stringify(response, null, 2));

      res.status(200).send('Test complete');
    } catch (error) {
      console.error(error);
      res.status(500).send('Internal Server Error');
    }
  },
);

/*********************************************************
 * Manually Trigger Cleaning Job Notifications
 *********************************************************/
export const manuallyTriggerCleaningJobNotifications = onRequest(
  {
    region: 'us-east4',
    nodeVersion: '20',
  },
  async (_req, res) => {
    try {
      const now = Timestamp.now().toDate();
      const startOfDay = new Date(now);
      startOfDay.setHours(0, 0, 0, 0);

      const endOfDay = new Date(now);
      endOfDay.setHours(23, 59, 59, 999);

      const jobsSnapshot = await db.collection('inletCleaningJobs').where('createdAt', '>=', Timestamp.fromDate(startOfDay)).where('createdAt', '<=', Timestamp.fromDate(endOfDay)).get();

      if (jobsSnapshot.empty) {
        res.status(200).send('No jobs created today.');
        return;
      }

      const processedInlets = new Set();
      let totalSent = 0;
      let totalFailed = 0;

      for (const jobDoc of jobsSnapshot.docs) {
        const job = jobDoc.data();
        const inletId = job.inletId;

        if (processedInlets.has(inletId)) continue;
        processedInlets.add(inletId);

        const inletDoc = await db.collection('inlets').doc(inletId).get();
        if (!inletDoc.exists) continue;

        const inlet = inletDoc.data();
        const subscribed = inlet.subscribed ?? [];

        let tokens = [];
        for (const userId of subscribed) {
          const userDoc = await db.collection('users').doc(userId).get();
          if (userDoc.exists) {
            tokens.push(...(userDoc.data().tokens ?? []));
          }
        }

        for (const token of tokens) {
          try {
            await messaging.send({
              token,
              notification: {
                title: 'Inlet Cleaning Needed',
                body: 'An Inlet you follow needs cleaning.',
              },
              android: { priority: 'high' },
            });
            totalSent++;
          } catch (e) {
            totalFailed++;
          }
        }
      }

      res.status(200).send(`Sent: ${totalSent}, Failed: ${totalFailed}`);
    } catch (err) {
      console.error(err);
      res.status(500).send('Internal Server Error');
    }
  },
);

/*********************************************************
 * Utility: Create Inlet Cleaning Job
 *********************************************************/
// Shared job-creation primitive so the single-inlet path (legacy trigger) and the
// batched group fan-out write identical documents. Adds the job + flips the inlet to
// 'cleaningScheduled' on the provided batch; extraInletUpdates lets the group path also
// stamp the per-inlet debounce atomically. Returns the new jobId.
function applyCleaningJobToBatch(batch, inletId, risk, extraInletUpdates = {}) {
  const jobRef = db.collection('inletCleaningJobs').doc();
  batch.set(jobRef, {
    inletId,
    createdAt: FieldValue.serverTimestamp(),
    status: 'pending',
    risk,
  });
  batch.update(db.collection('inlets').doc(inletId), {
    jobId: jobRef.id,
    status: 'cleaningScheduled',
    ...extraInletUpdates,
  });
  return jobRef.id;
}

async function createInletCleaningJob(inletId, risk) {
  const batch = db.batch();
  applyCleaningJobToBatch(batch, inletId, risk);
  await batch.commit();
}

export const MM_PER_INCH = 25.4;
export const RAIN_THRESHOLD_MM = 0.5 * MM_PER_INCH;

const parseValidTime = (validTime) => {
  const [startStr, durationStr] = validTime.split('/');

  const start = new Date(startStr);

  const hours = Number(durationStr.replace('PT', '').replace('H', ''));
  const end = new Date(start.getTime() + hours * 60 * 60 * 1000);

  return { start, end };
};

export const sumPrecipitationMM = (values, windowStart, windowEnd) => {
  let total = 0;

  for (const entry of values) {
    if (entry.value == null) continue;

    const { start, end } = parseValidTime(entry.validTime);

    const overlaps = start < windowEnd && end > windowStart;

    if (overlaps) {
      total += entry.value;
    }
  }

  return total;
};

/*********************************************************
 * Weather Check Function
 *********************************************************/
const WEATHER_CONCURRENCY = 5;

// Fetch + compute the 48h outlook for one NWS grid point, memoized by the
// gridpoint URL. Many inlets share a grid cell, so this collapses what used to
// be 3 NWS calls *per inlet* down to 3 calls *per unique grid cell*.
async function getWeatherForGridPoint(forecastUrl, gridPointUrl, now, window48h, cache) {
  if (cache.has(gridPointUrl)) return cache.get(gridPointUrl);

  const forecastRes = await fetch(forecastUrl);
  if (!forecastRes.ok) throw new Error(`NWS forecast API ${forecastRes.status}`);
  const forecastJson = await forecastRes.json();
  const nextPeriod = forecastJson.properties.periods?.[0];
  const risk = nextPeriod?.probabilityOfPrecipitation?.value || 0;

  const gridRes = await fetch(gridPointUrl);
  if (!gridRes.ok) throw new Error(`NWS gridData API ${gridRes.status}`);
  const gridJson = await gridRes.json();
  const quantitativePrecipitation = gridJson.properties.quantitativePrecipitation;

  let rainNext48MM = 0;
  if (quantitativePrecipitation?.values?.length) {
    rainNext48MM = sumPrecipitationMM(quantitativePrecipitation.values, now, window48h);
  }

  const result = {
    risk,
    rainNext48Inches: Number((rainNext48MM / MM_PER_INCH).toFixed(2)),
    heavyRainExpected: rainNext48MM >= RAIN_THRESHOLD_MM,
  };
  cache.set(gridPointUrl, result);
  return result;
}

async function checkWeatherStatus() {
  console.log('[checkWeatherStatus] Checking weather status...');
  const inlets = await db.collection('inlets').where('inletStatus', '==', 'ready').get();

  if (inlets.empty) {
    console.log('[checkWeatherStatus] No ready inlets to check.');
    return;
  }

  const now = new Date();
  const window48h = new Date(now.getTime() + 48 * 60 * 60 * 1000);

  // Two memo layers, both scoped to this single run:
  //  - pointCache: rounded lat/lng -> NWS forecast + gridpoint URLs
  //  - weatherCache: gridpoint URL -> computed { risk, rainNext48Inches, heavyRainExpected }
  const pointCache = new Map();
  const weatherCache = new Map();

  const limit = pLimit(WEATHER_CONCURRENCY);
  let updated = 0;
  let failed = 0;

  // Each inlet is isolated in its own try/catch so one bad NWS response can't
  // abort the whole run (the old sequential loop skipped every inlet after the
  // first failure).
  await Promise.allSettled(
    inlets.docs.map((doc) =>
      limit(async () => {
        const inlet = doc.data();
        const geo = inlet.geoLocation;
        if (!geo || geo.latitude == null || geo.longitude == null) {
          console.warn(`[checkWeatherStatus] Skipping ${doc.id}: missing geolocation.`);
          return;
        }

        try {
          const { forecastUrl, gridPointUrl } = await resolveNwsPoint(geo.latitude, geo.longitude, pointCache);
          const weather = await getWeatherForGridPoint(forecastUrl, gridPointUrl, now, window48h, weatherCache);

          await doc.ref.update({
            risk: weather.risk,
            rainNext48Inches: weather.rainNext48Inches,
            heavyRainExpected: weather.heavyRainExpected,
            weatherCheckedAt: FieldValue.serverTimestamp(),
          });
          updated++;

          // TODO: weatherPredictions logging intentionally disabled — we aren't
          // displaying prediction history yet, and writing one doc per inlet per
          // run was a large, unread write/storage cost. Re-enable behind a real
          // reader + a retention/TTL policy.
          // await db.collection('weatherPredictions').add({
          //   inletId: doc.id,
          //   risk: weather.risk,
          //   rainNext48Inches: weather.rainNext48Inches,
          //   heavyRainExpected: weather.heavyRainExpected,
          //   createdAt: FieldValue.serverTimestamp(),
          // });
        } catch (err) {
          failed++;
          console.error(`[checkWeatherStatus] Inlet ${doc.id} failed: ${err.message}`);
        }
      }),
    ),
  );

  console.log(`[checkWeatherStatus] Done. Updated ${updated}, failed ${failed}; ${inlets.size} ready inlets across ${weatherCache.size} unique grid points.`);
}

/*********************************************************
 * Geolocation-Grouped Weather — daily grouping
 *********************************************************/

// Greedy radius clustering of ready inlets into stable, center+radius groups.
// Stamps groupId on each inlet and upserts /inletGroups/*. Makes ZERO NWS calls —
// NWS point resolution is done lazily/persisted by checkGroupWeather.
async function rebuildGroups() {
  console.log('[rebuildInletGroups] Starting group rebuild...');

  // Existing groups let us carry forward a stable identity (and thus the already
  // resolved NWS URLs + debounce state) for centers that barely move run-to-run.
  const existingSnap = await db.collection('inletGroups').get();
  const existingGroups = existingSnap.docs.map((d) => ({ id: d.id, ...d.data() }));

  // Greedy pass over ready inlets, ordered by __name__ for day-to-day stability.
  const groups = []; // { centerLat, centerLng, sumLat, sumLng, count, memberIds }
  const PAGE_SIZE = 500;
  let lastDoc = null;
  let scanned = 0;

  while (true) {
    let query = db.collection('inlets').where('inletStatus', '==', 'ready').orderBy('__name__').limit(PAGE_SIZE);
    if (lastDoc) query = query.startAfter(lastDoc);

    const snap = await query.get();
    if (snap.empty) break;

    for (const doc of snap.docs) {
      const geo = doc.data().geoLocation;
      if (!geo || geo.latitude == null || geo.longitude == null) {
        console.warn(`[rebuildInletGroups] Skipping ${doc.id}: missing geolocation.`);
        continue;
      }

      const lat = geo.latitude;
      const lng = geo.longitude;
      scanned++;

      // Assign to the first group whose center is within the radius, else start one.
      // (For multi-region datasets, bucket candidate groups by geohash-5 prefix first
      //  to keep this near-linear — not needed at single-city scale.)
      let assigned = null;
      for (const g of groups) {
        if (geofire.distanceBetween([lat, lng], [g.centerLat, g.centerLng]) * 1000 <= GROUP_RADIUS_METERS) {
          assigned = g;
          break;
        }
      }

      if (!assigned) {
        assigned = { centerLat: lat, centerLng: lng, sumLat: 0, sumLng: 0, count: 0, memberIds: [] };
        groups.push(assigned);
      }

      assigned.sumLat += lat;
      assigned.sumLng += lng;
      assigned.count += 1;
      assigned.centerLat = assigned.sumLat / assigned.count;
      assigned.centerLng = assigned.sumLng / assigned.count;
      assigned.memberIds.push(doc.id);
    }

    lastDoc = snap.docs[snap.docs.length - 1];
    if (snap.size < PAGE_SIZE) break;
  }

  const now = Timestamp.now();
  const usedExistingIds = new Set();
  const groupDocs = []; // { id, memberIds, data }

  for (const g of groups) {
    // Reuse an existing group doc whose center is within GROUP_CENTER_MATCH_METERS.
    let matched = null;
    for (const eg of existingGroups) {
      if (usedExistingIds.has(eg.id) || !eg.centerGeo) continue;
      if (geofire.distanceBetween([g.centerLat, g.centerLng], [eg.centerGeo.latitude, eg.centerGeo.longitude]) * 1000 <= GROUP_CENTER_MATCH_METERS) {
        matched = eg;
        break;
      }
    }

    const id = matched ? matched.id : db.collection('inletGroups').doc().id;
    if (matched) usedExistingIds.add(matched.id);

    // merge:true preserves NWS URLs, last* weather, and lastJobBatchCreatedAt on
    // reused docs. New docs start without NWS URLs so checkGroupWeather resolves them.
    groupDocs.push({
      id,
      memberIds: g.memberIds,
      data: {
        centerGeo: new GeoPoint(g.centerLat, g.centerLng),
        centerGeoHash: geofire.geohashForLocation([g.centerLat, g.centerLng]),
        radiusMeters: GROUP_RADIUS_METERS,
        inletCount: g.count,
        rebuiltAt: now,
      },
    });
  }

  // Upsert group docs.
  await runBatched(groupDocs, (batch, gd) => {
    batch.set(db.collection('inletGroups').doc(gd.id), gd.data, { merge: true });
  });

  // Stamp groupId onto every member inlet (chunked across all groups).
  const memberUpdates = [];
  for (const gd of groupDocs) {
    for (const inletId of gd.memberIds) {
      memberUpdates.push({ inletId, groupId: gd.id });
    }
  }
  await runBatched(memberUpdates, (batch, { inletId, groupId }) => {
    batch.update(db.collection('inlets').doc(inletId), { groupId, groupAssignedAt: now });
  });

  // Delete existing groups that ended up with no members this run.
  const staleIds = existingGroups.filter((eg) => !usedExistingIds.has(eg.id)).map((eg) => eg.id);
  await runBatched(staleIds, (batch, id) => {
    batch.delete(db.collection('inletGroups').doc(id));
  });

  console.log(`[rebuildInletGroups] Done. Scanned ${scanned} ready inlets → ${groupDocs.length} groups (${staleIds.length} stale deleted, ${usedExistingIds.size} reused).`);
}

export const rebuildInletGroups = onSchedule(
  {
    schedule: '0 3 * * *',
    timeZone: 'America/New_York',
    region: 'us-east4',
    nodeVersion: '20',
    timeoutSeconds: 540,
    memory: '1GiB',
  },
  async () => {
    // Safe to run regardless of the flag: grouping is a prerequisite and makes no
    // NWS calls, so keeping membership fresh before cutover is harmless.
    await rebuildGroups();
    return null;
  },
);

/*********************************************************
 * Geolocation-Grouped Weather — hourly ping + fan-out
 *********************************************************/

// Union + de-duplicate the FCM tokens of every user subscribed to any of the given
// inlets, so a user following several inlets in one group gets a single push.
async function collectSubscriberTokens(inletDocs) {
  const userIds = new Set();
  for (const doc of inletDocs) {
    for (const uid of doc.data().subscribed ?? []) userIds.add(uid);
  }

  const ids = [...userIds];
  const tokens = new Set();
  const CHUNK = 300; // getAll fan-in
  for (let i = 0; i < ids.length; i += CHUNK) {
    const refs = ids.slice(i, i + CHUNK).map((id) => db.collection('users').doc(id));
    const userDocs = await db.getAll(...refs);
    for (const ud of userDocs) {
      if (!ud.exists) continue;
      for (const t of ud.data().tokens ?? []) tokens.add(t);
    }
  }

  return [...tokens];
}

// One weather ping per group, fanned out to member inlets. With createJobs=false
// this is a pure shadow run (weather + display writes only, no jobs/pushes).
async function runGroupWeather({ createJobs = true } = {}) {
  console.log(`[checkGroupWeather] Starting (createJobs=${createJobs})...`);

  const groupsSnap = await db.collection('inletGroups').get();
  if (groupsSnap.empty) {
    console.log('[checkGroupWeather] No groups to check. Run rebuildInletGroups first.');
    return;
  }

  const now = new Date();
  const window48h = new Date(now.getTime() + 48 * 60 * 60 * 1000);
  const nowTs = Timestamp.now();

  // Scoped memos in case two groups happen to share a point/grid cell this run.
  const pointCache = new Map();
  const weatherCache = new Map();
  const limit = pLimit(GROUP_WEATHER_CONCURRENCY);

  const stats = { groups: 0, jobsCreated: 0, pushSubscribers: 0, failures: 0 };

  await Promise.allSettled(
    groupsSnap.docs.map((groupDoc) =>
      limit(async () => {
        const group = groupDoc.data();
        const geo = group.centerGeo;
        if (!geo || geo.latitude == null || geo.longitude == null) {
          console.warn(`[checkGroupWeather] Group ${groupDoc.id} missing centerGeo, skipping.`);
          return;
        }

        try {
          // 1. Resolve the NWS point once and persist it — later runs skip /points.
          let { nwsForecastUrl: forecastUrl, nwsGridPointUrl: gridPointUrl } = group;
          if (!forecastUrl || !gridPointUrl) {
            const resolved = await resolveNwsPoint(geo.latitude, geo.longitude, pointCache);
            forecastUrl = resolved.forecastUrl;
            gridPointUrl = resolved.gridPointUrl;
            await groupDoc.ref.update({
              nwsForecastUrl: forecastUrl,
              nwsGridPointUrl: gridPointUrl,
              nwsResolvedAt: nowTs,
            });
          }

          // 2. Fetch weather once for the group.
          const weather = await getWeatherForGridPoint(forecastUrl, gridPointUrl, now, window48h, weatherCache);

          // 3. Persist group-level weather.
          await groupDoc.ref.update({
            lastRisk: weather.risk,
            lastRainNext48Inches: weather.rainNext48Inches,
            lastHeavyRainExpected: weather.heavyRainExpected,
            lastCheckedAt: nowTs,
          });
          stats.groups++;

          // 4. Fan out display fields to member inlets (batched, no per-inlet trigger).
          const membersSnap = await db.collection('inlets').where('groupId', '==', groupDoc.id).get();
          const memberDocs = membersSnap.docs;

          await runBatched(memberDocs, (batch, m) => {
            batch.update(m.ref, {
              risk: weather.risk,
              rainNext48Inches: weather.rainNext48Inches,
              heavyRainExpected: weather.heavyRainExpected,
              weatherCheckedAt: FieldValue.serverTimestamp(),
            });
          });

          if (!createJobs) return;

          // 5. Jobs + notifications, gated by a group-level 48h debounce (cheap early out).
          const groupDebouncePassed = !group.lastJobBatchCreatedAt || nowTs.toMillis() - group.lastJobBatchCreatedAt.toMillis() >= JOB_DEBOUNCE_MS;
          if (!(weather.heavyRainExpected && weather.risk > RISK_THRESHOLD && groupDebouncePassed)) return;

          // Only inlets whose own 48h debounce has passed get a fresh job.
          const affected = memberDocs.filter((m) => {
            const last = m.data().lastNotificationAndCleaningJobCreated;
            return !last || nowTs.toMillis() - last.toMillis() >= JOB_DEBOUNCE_MS;
          });

          // Stamp the group debounce regardless so we don't re-evaluate every hour.
          if (affected.length === 0) {
            await groupDoc.ref.update({ lastJobBatchCreatedAt: nowTs });
            return;
          }

          // Batch-create one job per affected inlet + stamp the per-inlet debounce.
          await runBatched(affected, (batch, m) => {
            applyCleaningJobToBatch(batch, m.id, weather.risk, { lastNotificationAndCleaningJobCreated: nowTs });
          });
          stats.jobsCreated += affected.length;

          // Record the group debounce BEFORE scheduling pushes, so a push failure
          // can't cause a duplicate job batch next run (mirrors the legacy ordering).
          await groupDoc.ref.update({ lastJobBatchCreatedAt: nowTs });

          const tokens = await collectSubscriberTokens(affected);
          if (tokens.length > 0) {
            const only = affected.length === 1 ? affected[0].data() : null;
            await schedulePushNotifications(db, {
              tokens,
              title: 'Inlet Cleaning Needed',
              body: only ? (only.address ? `The Inlet at ${only.address} needs cleaning.` : 'An Inlet you follow requires cleaning.') : 'Rain is expected — inlets you follow need cleaning.',
            });
            stats.pushSubscribers += tokens.length;
          }
        } catch (err) {
          stats.failures++;
          console.error(`[checkGroupWeather] Group ${groupDoc.id} failed: ${err.message}`);
        }
      }),
    ),
  );

  console.log(`[checkGroupWeather] Done. ${JSON.stringify(stats)}`);
}

export const checkGroupWeather = onSchedule(
  {
    schedule: '0 * * * *',
    timeZone: 'America/New_York',
    region: 'us-east4',
    nodeVersion: '20',
    timeoutSeconds: 540,
    memory: '512MiB',
  },
  async () => {
    if (!GROUP_WEATHER_ENABLED) {
      console.log('[checkGroupWeather] Skipped: GROUP_WEATHER_ENABLED is false. Use triggerGroupWeather for backfill/shadow runs.');
      return;
    }
    await runGroupWeather({ createJobs: true });
    return null;
  },
);

/*********************************************************
 * Geolocation-Grouped Weather — manual triggers
 *********************************************************/
export const triggerRebuildGroups = onRequest(
  {
    region: 'us-east4',
    nodeVersion: '20',
    timeoutSeconds: 540,
    memory: '1GiB',
  },
  async (_req, res) => {
    await rebuildGroups();
    res.send('Inlet groups rebuilt.');
  },
);

export const triggerGroupWeather = onRequest(
  {
    region: 'us-east4',
    nodeVersion: '20',
    timeoutSeconds: 540,
    memory: '512MiB',
  },
  async (req, res) => {
    // ?dryRun=1 → weather + display writes only, no jobs/pushes (shadow run).
    const dryRun = req.query.dryRun === '1' || req.query.dryRun === 'true';
    await runGroupWeather({ createJobs: !dryRun });
    res.send(`Group weather run complete (createJobs=${!dryRun}).`);
  },
);

const normalizeGeo = (lat, lng) => {
  return `${lat.toFixed(6)},${lng.toFixed(6)}`;
};

export const manualNormalizeGeo = onRequest(
  {
    region: 'us-east4',
    nodeVersion: '20',
    timeoutSeconds: 540,
    memory: '1GiB',
  },
  async (_, res) => {
    const PAGE_SIZE = 500;
    let lastDoc = null;
    let totalUpdated = 0;

    while (true) {
      let query = db.collection('inlets').orderBy('__name__').limit(PAGE_SIZE);

      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snap = await query.get();

      console.log(`Updating ${snap.size} documents...`);

      if (snap.empty) break;

      const batch = db.batch();
      for (const doc of snap.docs) {
        const data = doc.data();
        if (data.geoHash) continue;

        const geo = data.geoLocation;
        if (!geo || geo.latitude == null || geo.longitude == null) {
          console.warn(`Skipping ${doc.id} due to missing geolocation.`);
          continue;
        }

        const geoHash = normalizeGeo(geo.latitude, geo.longitude);

        batch.update(doc.ref, {
          geoHash,
        });

        totalUpdated++;
      }
      await batch.commit();
      lastDoc = snap.docs[snap.docs.length - 1];
    }

    res.json({
      success: true,
      updated: totalUpdated,
    });
  },
);

export const backfillGHash = onRequest(
  {
    region: 'us-east4',
    nodeVersion: '20',
    timeoutSeconds: 540,
    memory: '1GiB',
  },
  async (_, res) => {
    const PAGE_SIZE = 500;
    let lastDoc = null;
    let totalUpdated = 0;

    while (true) {
      let query = db.collection('inlets').orderBy('__name__').limit(PAGE_SIZE);

      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snap = await query.get();

      if (snap.empty) break;

      const batch = db.batch();
      for (const doc of snap.docs) {
        const data = doc.data();
        if (data.gHash) continue;

        const geo = data.geoLocation;
        if (!geo || geo.latitude == null || geo.longitude == null) {
          console.warn(`Skipping ${doc.id} due to missing geolocation.`);
          continue;
        }

        const gHash = geofire.geohashForLocation([geo.latitude, geo.longitude]);

        batch.update(doc.ref, { gHash });
        totalUpdated++;
      }

      await batch.commit();
      lastDoc = snap.docs[snap.docs.length - 1];
    }

    res.json({ success: true, updated: totalUpdated });
  },
);

async function streamImageToGCS(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Image fetch failed: ${res.status}`);

  const contentType = res.headers.get('content-type') || 'application/octet-stream';

  const ext = contentType.split('/')[1]?.split(';')[0] || 'jpeg';
  const filename = `${crypto.randomUUID()}.${ext}`;

  const path = `inlet-photos/${filename}`;
  const file = bucket.file(path);

  await pipeline(
    res.body,
    file.createWriteStream({
      resumable: false,
      metadata: { contentType, cacheControl: 'public, max-age=31536000' },
    }),
  );

  await file.makePublic();

  return filename;
}

const ROWS_PER_RUN = 10;
const CONCURRENCY = 3;
const LEASE_MS = 5 * 60 * 1000;

export const processImports = onDocumentWritten(
  {
    document: 'imports/{importId}',
    region: 'us-east4',
    nodeVersion: '20',
    memory: '2GiB',
    timeoutSeconds: 540,
    concurrency: 1,
    maxInstances: 5,
  },
  async (event) => {
    const after = event.data?.after;
    if (!after?.exists) return;

    const importRef = after.ref;
    const importData = after.data();
    const inletRef = db.collection('inlets');

    // HARD GUARDS
    if (importData.status !== 'processing') return;
    if (importData.active !== true) return;

    // Lock immediately
    await importRef.update({
      active: false,
      updatedAt: FieldValue.serverTimestamp(),
    });

    const rowsSnap = await importRef.collection('rows').where('status', '==', 'queued').limit(ROWS_PER_RUN).get();

    if (rowsSnap.empty) {
      await importRef.update({
        status: 'done',
        completedAt: FieldValue.serverTimestamp(),
      });
      return;
    }

    const limit = pLimit(CONCURRENCY);
    const now = Date.now();

    await Promise.allSettled(
      rowsSnap.docs.map((doc) =>
        limit(async () => {
          const rowRef = doc.ref;
          const row = doc.data();

          await rowRef.update({
            status: 'processing',
            leaseUntil: Timestamp.fromMillis(now + LEASE_MS),
            attempts: FieldValue.increment(1),
          });

          try {
            let images = [];

            if (row.image) {
              images.push(await streamImageToGCS(row.image));
            }

            // Deduplication logic
            const lat = Number(row.latitude);
            const lng = Number(row.longitude);
            const geohash = normalizeGeo(lat, lng);
            const gHash = geofire.geohashForLocation([lat, lng]);

            const existingSnap = await inletRef
              .where('nickName', '==', row.name)
              .where('description', '==', row.description || '')
              .where('geoHash', '==', geohash)
              .limit(1)
              .get();

            if (!existingSnap.empty) {
              const existingDoc = existingSnap.docs[0];
              const existingData = existingDoc.data();

              let finalImages = [];

              if (existingData.inletStatus === 'photo_needed') {
                finalImages = images;
              } else {
                finalImages = Array.from(new Set([...(existingData.images || []), ...images]));
              }

              const finalAddress = (existingData.address && existingData.address.trim()) || (row.address && row.address.trim()) || '';
              const isReady = finalImages.length > 0 && finalAddress.length > 0;

              const updatePayload = {
                images: finalImages,
                gHash,
              };

              if (!existingData.address && finalAddress) {
                updatePayload.address = finalAddress;
              }

              if (isReady && existingData.inletStatus !== 'ready') {
                updatePayload.inletStatus = 'ready';
              }

              await existingDoc.ref.update(updatePayload);
            } else {
              await inletRef.add({
                nickName: row.name,
                address: row.address || '',
                description: row.description || '',
                images: images,
                geoLocation: new GeoPoint(Number(row.latitude), Number(row.longitude)),
                geoHash: geohash,
                gHash,
                inletStatus: row?.address.trim() && images.length > 0 ? 'ready' : 'photo_needed',
              });
            }

            await rowRef.update({
              status: 'done',
            });

            await importRef.update({
              processedRows: FieldValue.increment(1),
              successRows: FieldValue.increment(1),
            });
          } catch (err) {
            console.error('Row failed:', err);

            await rowRef.update({
              status: 'error',
              lastError: err.message,
            });

            await importRef.update({
              processedRows: FieldValue.increment(1),
              failedRows: FieldValue.increment(1),
            });
          }
        }),
      ),
    );

    await importRef.update({
      active: true,
      updatedAt: FieldValue.serverTimestamp(),
    });
  },
);
