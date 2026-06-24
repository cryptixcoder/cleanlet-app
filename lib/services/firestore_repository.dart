import 'dart:async';

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:google_maps_flutter/google_maps_flutter.dart';

import '../models/inlet.dart';
import '../models/job.dart';
import '../models/user.dart';
import '../utils/geohash_utils.dart';
import 'firebase_auth_repository.dart';
import 'firestore_data_source.dart';

class FirestoreRepository {
  const FirestoreRepository(this._dataSource);

  final FirestoreDataSource _dataSource;

  // WARNING: streams the ENTIRE `inlets` collection unbounded and re-emits on
  // every change. Do NOT use this in the UI — it will load every inlet in the
  // database. Use `viewportMapProvider` / `fetchInletsInBounds` instead.
  Stream<List<Inlet>> watchInlets() => _dataSource.watchCollection(
        path: 'inlets',
        builder: (data, documentId) => Inlet.fromMap(data, documentId),
      );

  Future<List<Inlet>> fetchInletsInBounds(
      LatLngBounds bounds, double zoom, Map<String, List<Inlet>> regionCache) async {
    final precision = GeohashUtils.precisionForZoom(zoom);
    final centerLat =
        (bounds.northeast.latitude + bounds.southwest.latitude) / 2;
    final centerLng =
        (bounds.northeast.longitude + bounds.southwest.longitude) / 2;

    final centerHash = GeohashUtils.encode(centerLat, centerLng, precision);
    final cells = GeohashUtils.getNeighborsAndSelf(centerHash);

    final seen = <String>{};
    final results = <Inlet>[];
    final uncachedCells = <String>[];

    // Serve already-visited cells from memory; only query the rest.
    for (final cell in cells) {
      if (regionCache.containsKey(cell)) {
        for (final inlet in regionCache[cell]!) {
          if (seen.add(inlet.referenceId)) results.add(inlet);
        }
      } else {
        uncachedCells.add(cell);
      }
    }

    if (uncachedCells.isNotEmpty) {
      final futures = uncachedCells
          .map((cell) => FirebaseFirestore.instance
              .collection('inlets')
              .where('gHash', isGreaterThanOrEqualTo: cell)
              .where('gHash', isLessThan: '$cell~')
              .get())
          .toList();

      final snapshots = await Future.wait(futures);

      for (int i = 0; i < uncachedCells.length; i++) {
        final cellInlets = <Inlet>[];
        for (final doc in snapshots[i].docs) {
          try {
            final inlet = Inlet.fromMap(doc.data(), doc.id);
            cellInlets.add(inlet);
            if (seen.add(doc.id)) results.add(inlet);
          } catch (_) {
            // Skip malformed documents.
          }
        }
        regionCache[uncachedCells[i]] = cellInlets;
      }
    }

    // Geohash cells extend beyond the visible viewport, so restrict the
    // returned inlets to what's actually on screen. This keeps the marker
    // count tied to what the user sees rather than the (larger) query region.
    return results
        .where((inlet) => bounds.contains(
              LatLng(inlet.geoLocation.latitude, inlet.geoLocation.longitude),
            ))
        .toList();
  }

  Stream<Inlet> watchInlet({required InletID inletID}) =>
      _dataSource.watchDocument(
        path: 'inlets/$inletID',
        builder: (data, documentId) => Inlet.fromMap(data, documentId),
      );
  Stream<Job> watchJob({required String jobId}) => _dataSource.watchDocument(
        path: 'inletCleaningJobs/$jobId',
        builder: (data, documentId) => Job.fromMap(data, documentId),
      );

  Stream<CleanletUser> watchUser({required String userID}) =>
      _dataSource.watchDocument(
        path: 'users/$userID',
        builder: (data, documentId) => CleanletUser.fromMap(data, documentId),
      );

  Future<void> updateInlet(String referenceId,
          {required Map<String, dynamic> data}) =>
      _dataSource.setData(
        path: 'inlets/$referenceId',
        data: data,
      );

  Future<void> updateUser(String userId,
          {required Map<String, dynamic> data}) =>
      _dataSource.setData(
        path: 'users/$userId',
        data: data,
      );

  Future<void> updateJob(String jobId, {required Map<String, dynamic> data}) =>
      _dataSource.setData(
        path: 'inletCleaningJobs/$jobId',
        data: data,
      );
}

final databaseProvider = Provider<FirestoreRepository>((ref) {
  return FirestoreRepository(ref.watch(firestoreDataSourceProvider));
});

// WARNING: do NOT consume this in the UI. It streams the entire `inlets`
// collection unbounded (see watchInlets above). The map uses
// `viewportMapProvider` for bounded, viewport-scoped loading instead.
final inletsStreamProvider = StreamProvider.autoDispose<List<Inlet>>((ref) {
  final database = ref.watch(databaseProvider);
  return database.watchInlets();
});

final inletStreamProvider =
    StreamProvider.autoDispose.family<Inlet, InletID>((ref, inletId) {
  final database = ref.watch(databaseProvider);
  return database.watchInlet(inletID: inletId);
});

final jobStreamProvider =
    StreamProvider.autoDispose.family<Job, JobID>((ref, jobId) {
  final database = ref.watch(databaseProvider);
  return database.watchJob(jobId: jobId);
});

final userProvider = StreamProvider.autoDispose((ref) {
  final user = ref.watch(authStateChangesProvider).value;

  if (user == null) {
    throw AssertionError('User can\'t be null');
  }
  final database = ref.watch(databaseProvider);
  return database.watchUser(userID: user.uid);
});

final autoUpdateUserProvider =
    FutureProvider.family<void, User>((ref, user) async {
  final db = ref.watch(databaseProvider);

  final Map<String, dynamic> userData = {
    'displayName': user.displayName,
    'photoURL': user.photoURL,
    'email': user.email!,
    'appLastUsed': FieldValue.serverTimestamp(),
  };

  await db.updateUser(user.uid, data: userData);
});

final autoUpdateUserListenerProvider = Provider<void>((ref) {
  final userAsyncValue = ref.watch(userChangesProvider);

  userAsyncValue.maybeWhen(
    data: (user) {
      if (user != null) {
        ref.read(autoUpdateUserProvider(user));
      }
    },
    orElse: () {},
  );
});

// final listenToUserChangesProvider = StreamProvider<void>((ref) {
//   return ref.watch(userChangesProvider).asyncMap((user) {
//     if (user != null) {
//       return ref.read(autoUpdateUserProvider(user));
//     } else {
//       return Stream.value(null);
//     }
//   });
// });

class ViewportMapNotifier extends AutoDisposeAsyncNotifier<List<Inlet>> {
  int _requestId = 0;
  final Map<String, List<Inlet>> _regionCache = {};

  @override
  FutureOr<List<Inlet>> build() => [];

  Future<void> updateViewport(LatLngBounds bounds, double zoom) async {
    if (zoom < 10) {
      state = const AsyncData([]);
      return;
    }
    final id = ++_requestId;
    state = const AsyncLoading();
    try {
      final inlets = await ref
          .read(databaseProvider)
          .fetchInletsInBounds(bounds, zoom, _regionCache);
      if (id == _requestId) state = AsyncData(inlets);
    } catch (e, st) {
      if (id == _requestId) state = AsyncError(e, st);
    }
  }
}

final viewportMapProvider =
    AsyncNotifierProvider.autoDispose<ViewportMapNotifier, List<Inlet>>(
  ViewportMapNotifier.new,
);
