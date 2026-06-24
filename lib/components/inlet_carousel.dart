import 'package:cached_network_image/cached_network_image.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_carousel_widget/flutter_carousel_widget.dart';
import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_storage/firebase_storage.dart';

class InletCarousel extends StatelessWidget {
  final String referenceId;

  const InletCarousel({
    Key? key,
    required this.referenceId,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<List<String>>(
      future: getImageUrlsFromFirestore(),
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return CircularProgressIndicator();
        } else if (snapshot.hasError) {
          return Text('Error: ${snapshot.error}');
        } else if (!snapshot.hasData || snapshot.data!.isEmpty) {
          return Text('No images available.');
        } else {
          final imageUrls = snapshot.data!;

          return FlutterCarousel(
            options: FlutterCarouselOptions(
              height: 200,
              showIndicator: true,
              slideIndicator: CircularSlideIndicator(),
              enableInfiniteScroll: true,
            ),
            items: imageUrls.map((imageUrl) {
              return Builder(
                builder: (BuildContext context) {
                  final width = MediaQuery.of(context).size.width;
                  final dpr = MediaQuery.of(context).devicePixelRatio;
                  return CachedNetworkImage(
                    imageUrl: imageUrl,
                    fit: BoxFit.cover,
                    width: width,
                    // Decode to display size to cap memory; disk cache makes
                    // revisits load instantly instead of re-downloading.
                    memCacheWidth: (width * dpr).round(),
                    placeholder: (context, url) =>
                        const Center(child: CircularProgressIndicator()),
                    errorWidget: (context, url, error) =>
                        const Center(child: Icon(Icons.broken_image)),
                  );
                },
              );
            }).toList(),
          );
        }
      },
    );
  }

  Future<List<String>> getImageUrlsFromFirestore() async {
    try {
      final DocumentSnapshot docSnapshot = await FirebaseFirestore.instance
          .collection('inlets') // Replace with your Firestore collection name
          .doc(referenceId) // Use the referenceId as the document ID
          .get();

      if (docSnapshot.exists) {
        final data = docSnapshot.data() as Map<String, dynamic>?;

        if (data != null && data.containsKey('images')) {
          final List<dynamic> imageList = data['images'] as List<dynamic>;

          final List<String> imageUrls = await Future.wait(
            imageList.map((image) async {
              final String imageFileName = image.toString();
              final String imageUrl = await FirebaseStorage.instance.ref('inlet-photos/$imageFileName').getDownloadURL();
              return imageUrl;
            }),
          );
          return imageUrls;
        } else {
          return [];
        }
      } else {
        return [];
      }
    } catch (e) {
      if (kDebugMode) {
        print('Error fetching data: $e');
      }
      return [];
    }
  }
}
