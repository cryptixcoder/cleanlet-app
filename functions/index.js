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
    schedule: 'every 1 minutes',
    region: 'us-east4',
    nodeVersion: '20',
  },
  async () => {
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

    let tokens = [];

    for (const userId of newValue.subscribed ?? []) {
      const userDoc = await db.collection('users').doc(userId).get();
      if (userDoc.exists && userDoc.data().tokens) {
        tokens.push(...userDoc.data().tokens);
      }
    }

    await schedulePushNotification(db, {
      tokens,
      title: 'Inlet Cleaning Needed',
      body: oldValue?.address ? `The Inlet at ${oldValue.address} needs cleaning.` : 'An Inlet you follow requires cleaning.',
    });

    await db.collection('inlets').doc(event.params.inletId).update({ lastNotificationAndCleaningJobCreated: now });

    // if (!lastNotification || now.toDate().getTime() - lastNotification.toDate().getTime() >= 48 * 60 * 60 * 1000) {
    //   if (oldValue.risk !== newValue.risk && newValue.risk > 35) {
    //     console.log('High risk detected, creating cleaning job...');
    //     await createInletCleaningJob(event.params.inletId, newValue.risk);

    //     let tokens = [];

    //     for (const userId of newValue.subscribed ?? []) {
    //       const userDoc = await db.collection('users').doc(userId).get();
    //       if (userDoc.exists && userDoc.data().tokens) {
    //         tokens.push(...userDoc.data().tokens);
    //       }
    //     }

    //     await schedulePushNotification(db, {
    //       tokens,
    //       title: 'Inlet Cleaning Needed',
    //       body: oldValue?.address ? `The Inlet at ${oldValue.address} needs cleaning.` : 'An Inlet you follow requires cleaning.',
    //     });

    //     if (tokens.length > 0) {
    //       const message = {
    //         tokens,
    //         notification: {
    //           title: 'Inlet Cleaning Needed',
    //           body: oldValue?.address ? `The Inlet at ${oldValue.address} needs cleaning.` : 'An Inlet you follow requires cleaning.',
    //         },
    //         android: { priority: 'high' },
    //       };

    //       const response = await messaging.sendEachForMulticast(message);
    //       response.responses.forEach((r, i) => {
    //         if (r.success) console.log(`Message to ${tokens[i]} succeeded`);
    //         else console.error(`Message failed: ${r.error?.message}`);
    //       });
    //     }

    //     await db.collection('inlets').doc(event.params.inletId).update({ lastNotificationAndCleaningJobCreated: now });
    //   }
    // }
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
async function createInletCleaningJob(inletId, risk) {
  const ref = await db.collection('inletCleaningJobs').add({
    inletId,
    createdAt: FieldValue.serverTimestamp(),
    status: 'pending',
    risk,
  });
  await db.collection('inlets').doc(inletId).update({
    jobId: ref.id,
    status: 'cleaningScheduled',
  });
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
async function checkWeatherStatus() {
  console.log('Checking weather status...');
  const inlets = await db.collection('inlets').get();

  const now = new Date();
  const window24h = new Date(now.getTime() + 24 * 60 * 60 * 1000);
  const window48h = new Date(now.getTime() + 48 * 60 * 60 * 1000);

  for (const doc of inlets.docs) {
    const inlet = doc.data();
    const { latitude, longitude } = inlet.geoLocation;

    const pointRes = await fetch(`https://api.weather.gov/points/${latitude},${longitude}`);
    const pointJson = await pointRes.json();
    const forecastUrl = pointJson.properties.forecast;
    const gridPointUrl = pointJson.properties.forecastGridData;

    const forecastRes = await fetch(forecastUrl);
    const forecastJson = await forecastRes.json();
    const periods = forecastJson.properties.periods;
    const nextPeriod = periods[0];
    const risk = nextPeriod.probabilityOfPrecipitation?.value || 0;

    const gridPontRes = await fetch(gridPointUrl);
    const gridPointJson = await gridPontRes.json();

    const quantitativePrecipitation = gridPointJson.properties.quantitativePrecipitation;

    let rain24to48MM = 0;
    let rainNext48MM = 0;

    if (quantitativePrecipitation?.values?.length) {
      rainNext48MM = sumPrecipitationMM(quantitativePrecipitation.values, now, window48h);

      rain24to48MM = sumPrecipitationMM(quantitativePrecipitation.values, window24h, window48h);
    }

    const rainNext48Inches = rainNext48MM / MM_PER_INCH;
    const heavyRainExpected = rainNext48MM >= RAIN_THRESHOLD_MM;

    await doc.ref.update({
      risk,
      rainNext48Inches: Number(rainNext48Inches.toFixed(2)),
      heavyRainExpected,
      weatherCheckedAt: FieldValue.serverTimestamp(),
    });

    if (inlet.inletStatus === 'ready') {
      await db.collection('weatherPredictions').add({
        inletId: doc.id,
        risk,
        rainNext48Inches: Number(rainNext48Inches.toFixed(2)),
        heavyRainExpected,
        createdAt: FieldValue.serverTimestamp(),
      });
    }
  }
}

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
