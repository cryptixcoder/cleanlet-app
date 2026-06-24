import 'dart:async';

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:firebase_messaging/firebase_messaging.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:google_maps_flutter/google_maps_flutter.dart';
import 'package:shared_preferences/shared_preferences.dart';

import '../components/current_inlets_watched.dart';
import '../models/inlet.dart';
import '../services/firestore_repository.dart';
import '../services/geolocation.dart';
import '../components/carousel_modal_widget.dart';
import 'inlet_view.dart';
import 'inlet_photo_needed.dart';
import 'inlet_admin_review.dart';

class HomePage extends ConsumerStatefulWidget {
  const HomePage({super.key});

  @override
  ConsumerState<HomePage> createState() => _HomePageState();
}

class _HomePageState extends ConsumerState<HomePage> {
  FirebaseMessaging messaging = FirebaseMessaging.instance;

  final List<String> messages = ["Hi and welcome to Cleanlet! Thank you for supporting this project! Here are a few things that you should know:", "Be safe: Always follow the cleaning guidelines and clean only when it feels safe to you. You can find the guidelines in the (?) section of the app.", "Feel free to let us know of any bugs or feedback using the button in the top right menu.", "Read the instructions on how to use the app in the (?) section."];

  GoogleMapController? _mapController;
  Timer? _debounce;
  double _currentZoom = 19.0;
  final Map<String, Marker> _markerCache = {};
  final Map<String, String?> _markerStatusCache = {};

  // Snapshot of the marker set handed to GoogleMap. Rebuilt only when markers
  // actually change (in _applyViewportInlets), not on every widget rebuild.
  Set<Marker> _markers = {};

  // Pre-created once — reused for every marker build.
  final BitmapDescriptor _iconGreen = BitmapDescriptor.defaultMarkerWithHue(BitmapDescriptor.hueGreen);
  final BitmapDescriptor _iconRed = BitmapDescriptor.defaultMarkerWithHue(BitmapDescriptor.hueRed);
  final BitmapDescriptor _iconOrange = BitmapDescriptor.defaultMarkerWithHue(BitmapDescriptor.hueOrange);

  BitmapDescriptor _getMarkerIcon(String inletStatus) {
    switch (inletStatus) {
      case 'ready':
        return _iconGreen;
      case 'photo_needed':
        return _iconRed;
      default:
        return _iconOrange;
    }
  }

  void _navigateToInletPage(BuildContext context, Inlet inlet) {
    switch (inlet.inletStatus) {
      case 'ready':
        Navigator.push(context, MaterialPageRoute(builder: (context) => InletView(inlet: inlet)));
        break;
      case 'photo_needed':
        Navigator.push(context, MaterialPageRoute(builder: (context) => InletPhotoNeed(inlet: inlet)));
        break;
      default:
        Navigator.push(context, MaterialPageRoute(builder: (context) => InletAdminReview(inlet: inlet)));
        break;
    }
  }

  Future<bool> checkFirstSeen() async {
    SharedPreferences prefs = await SharedPreferences.getInstance();
    bool seen = (prefs.getBool('seen') ?? false);
    if (!seen) {
      await prefs.setBool('seen', true);
      return true;
    }
    return false;
  }

  void registerNotification() async {
    NotificationSettings settings = await messaging.requestPermission(
      alert: true,
      badge: true,
      provisional: false,
      sound: true,
    );

    FirebaseMessaging.onMessage.listen((RemoteMessage message) {
      if (kDebugMode) {
        print('Got a message whilst in the foreground!');
        print('Message data: ${message.data}');
      }
      showDialog(
          context: context,
          builder: (BuildContext context) {
            return AlertDialog(
              title: Text(message.notification!.title!),
              content: Text(message.notification!.body!),
              actions: [
                TextButton(
                  child: const Text("Ok"),
                  onPressed: () {
                    Navigator.of(context).pop();
                  },
                )
              ],
            );
          });

      if (message.notification != null) {
        if (kDebugMode) {
          print('Message also contained a notification: ${message.notification}');
        }
      }
    });

    if (settings.authorizationStatus == AuthorizationStatus.authorized) {
      if (kDebugMode) {
        print('User granted permission');
      }
    } else {
      if (kDebugMode) {
        print('User declined or has not accepted permission');
      }
    }
  }

  Future<void> saveTokenToDatabase(String token) async {
    String? userId = FirebaseAuth.instance.currentUser?.uid;
    if (userId == null) return;

    final userDoc = FirebaseFirestore.instance.collection('users').doc(userId);

    bool success = await updateUserToken(userDoc, token);

    if (!success) {
      await Future.delayed(const Duration(seconds: 5));
      await updateUserToken(userDoc, token);
    }
  }

  Future<bool> updateUserToken(DocumentReference userDoc, String token) async {
    try {
      final docSnapshot = await userDoc.get();

      if (docSnapshot.exists) {
        await userDoc.update({
          'tokens': FieldValue.arrayUnion([token]),
        });
        return true;
      } else {
        return false;
      }
    } catch (e) {
      if (kDebugMode) {
        print("Error updating token: $e");
      }
      return false;
    }
  }

  Future<void> setupToken() async {
    String? token = await FirebaseMessaging.instance.getToken();
    await saveTokenToDatabase(token!);
    FirebaseMessaging.instance.onTokenRefresh.listen(saveTokenToDatabase);
  }

  @override
  void initState() {
    super.initState();
    registerNotification();
    setupToken();
  }

  @override
  void dispose() {
    _debounce?.cancel();
    _mapController?.dispose();
    super.dispose();
  }

  void _onCameraMove(CameraPosition position) {
    _currentZoom = position.zoom;
  }

  void _onCameraIdle() {
    _debounce?.cancel();
    _debounce = Timer(const Duration(milliseconds: 250), () async {
      if (_mapController == null) return;
      final bounds = await _mapController!.getVisibleRegion();
      await ref.read(viewportMapProvider.notifier).updateViewport(bounds, _currentZoom);
    });
  }

  void _applyViewportInlets(List<Inlet> inlets) {
    final incoming = {for (final i in inlets) i.referenceId: i};
    bool changed = false;

    // Remove markers that left the viewport.
    _markerCache.removeWhere((id, _) {
      if (!incoming.containsKey(id)) {
        _markerStatusCache.remove(id);
        changed = true;
        return true;
      }
      return false;
    });

    // Add or rebuild only markers whose status changed.
    for (final inlet in inlets) {
      if (_markerStatusCache[inlet.referenceId] == inlet.inletStatus && _markerCache.containsKey(inlet.referenceId)) {
        continue;
      }
      _markerCache[inlet.referenceId] = Marker(
        markerId: MarkerId(inlet.referenceId),
        position: LatLng(inlet.geoLocation.latitude, inlet.geoLocation.longitude),
        icon: _getMarkerIcon(inlet.inletStatus ?? 'unknown'),
        onTap: () => _navigateToInletPage(context, inlet),
      );
      _markerStatusCache[inlet.referenceId] = inlet.inletStatus;
      changed = true;
    }

    if (changed) {
      _markers = _markerCache.values.toSet();
      setState(() {});
    }
  }

  @override
  Widget build(BuildContext context) {
    ref.listen<AsyncValue<List<Inlet>>>(
      viewportMapProvider,
      (_, next) => next.whenData(_applyViewportInlets),
    );

    final isLoadingInlets = ref.watch(viewportMapProvider).isLoading;

    return Scaffold(
      appBar: AppBar(
        title: Consumer(
          builder: (context, ref, child) {
            final user = ref.watch(userProvider);
            return user.when(
              data: (user) {
                final text = (user.displayName != null && user.displayName!.isNotEmpty) ? user.displayName! : user.email;
                return Text(text);
              },
              loading: () => const CircularProgressIndicator(),
              error: (err, stack) => const Text('Error'),
            );
          },
        ),
        leading: Consumer(
          builder: (context, ref, child) {
            final user = ref.watch(userProvider);
            return user.when(
              data: (user) {
                return user.photoURL != null
                    ? Padding(
                        padding: const EdgeInsets.all(8.0),
                        child: CircleAvatar(
                          backgroundImage: NetworkImage(user.photoURL!),
                        ),
                      )
                    : const Icon(Icons.person);
              },
              loading: () => const CircularProgressIndicator(),
              error: (err, stack) => const Text('Error'),
            );
          },
        ),
        actions: [
          IconButton(
              onPressed: () {
                showDialog(
                    context: context,
                    builder: (BuildContext context) {
                      return CarouselModalWidget(messages: messages);
                    });
              },
              icon: const Icon(Icons.help_outline_rounded)),
          IconButton(
              onPressed: () {
                Navigator.pushNamed(context, '/settings');
              },
              icon: const Icon(Icons.menu_rounded))
        ],
      ),
      bottomNavigationBar: BottomAppBar(
        color: Theme.of(context).colorScheme.primary,
        child: const Padding(
          padding: EdgeInsets.all(10.0),
          child: Row(
            mainAxisAlignment: MainAxisAlignment.spaceEvenly,
            children: [
              CurrentInletsWatched(),
            ],
          ),
        ),
      ),
      body: SafeArea(
        child: Stack(
          children: [
            _buildMap(),
            if (isLoadingInlets) const _LoadingInletsIndicator(),
          ],
        ),
      ),
    );
  }

  Widget _buildMap() {
    return Consumer(builder: (context, ref, _) {
      final position = ref.watch(positionProvider);
      return position.when(
        data: (currentPosition) => GoogleMap(
          mapType: MapType.normal,
          initialCameraPosition: CameraPosition(
            target: LatLng(currentPosition.latitude, currentPosition.longitude),
            zoom: _currentZoom,
          ),
          onMapCreated: (GoogleMapController controller) async {
            _mapController = controller;
            if (await checkFirstSeen()) {
              WidgetsBinding.instance.addPostFrameCallback((_) {
                showDialog(
                    context: context,
                    builder: (BuildContext context) {
                      return CarouselModalWidget(messages: messages);
                    });
              });
            }
            _onCameraIdle();
          },
          onCameraMove: _onCameraMove,
          onCameraIdle: _onCameraIdle,
          markers: _markers,
          myLocationEnabled: true,
          myLocationButtonEnabled: true,
        ),
        error: (error, stack) => Text('Error: ${error.toString()}'),
        loading: () => const Text('Loading...'),
      );
    });
  }
}

/// Centered, non-interactive overlay shown while a viewport inlet load is in
/// progress, so the map doesn't appear frozen. Wrapped in IgnorePointer so the
/// user can keep panning the map underneath while inlets load.
class _LoadingInletsIndicator extends StatelessWidget {
  const _LoadingInletsIndicator();

  @override
  Widget build(BuildContext context) {
    return IgnorePointer(
      child: Center(
        child: Material(
          elevation: 4,
          borderRadius: BorderRadius.circular(24),
          color: Theme.of(context).colorScheme.surface,
          child: const Padding(
            padding: EdgeInsets.symmetric(horizontal: 20, vertical: 12),
            child: Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                SizedBox(
                  width: 18,
                  height: 18,
                  child: CircularProgressIndicator(strokeWidth: 2),
                ),
                SizedBox(width: 12),
                Text('Loading Inlets'),
              ],
            ),
          ),
        ),
      ),
    );
  }
}
