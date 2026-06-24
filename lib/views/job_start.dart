import 'dart:io';

import 'package:cleanlet/views/test.dart';
import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_storage/firebase_storage.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:image_picker/image_picker.dart';

import '../models/inlet.dart';
import '../services/firestore_repository.dart';

class CleaningPhotoView extends ConsumerStatefulWidget {
  final Inlet inlet;
  final String photoToTake;
  const CleaningPhotoView(this.inlet, this.photoToTake, {super.key});

  @override
  ConsumerState<CleaningPhotoView> createState() => _JobStartPageState();
}

class _JobStartPageState extends ConsumerState<CleaningPhotoView> {
  File? _image;
  final _picker = ImagePicker();
  final storageRef = FirebaseStorage.instance.ref();

  bool _isUploading = false;
  double _uploadProgress = 0;

  // Cleaning Before/After photos are reviewed by admins, so keep them sharp:
  // cap the dimension high and use light compression rather than aggressive
  // downscaling. Still far smaller than a raw multi-MP camera capture.
  static const _maxImageWidth = 2560.0;
  static const _imageQuality = 90;

  // Implementing the image picker
  Future<void> _openImagePicker(ImageSource source) async {
    final XFile? pickedImage = await _picker.pickImage(
      source: source,
      maxWidth: _maxImageWidth,
      imageQuality: _imageQuality,
    );
    if (pickedImage != null) {
      setState(() {
        _image = File(pickedImage.path);
      });
    }
  }

  Future<void> _uploadPhoto() async {
    if (_image == null) return;

    setState(() {
      _isUploading = true;
      _uploadProgress = 0;
    });

    try {
      final uploadRef = storageRef
          .child('cleaning-images')
          .child(widget.inlet.jobId)
          .child('${widget.photoToTake}.jpg');

      final task = uploadRef.putFile(
        _image!,
        SettableMetadata(
          contentType: 'image/jpeg',
          cacheControl: 'public, max-age=31536000',
        ),
      );

      task.snapshotEvents.listen((snapshot) {
        if (snapshot.totalBytes > 0 && mounted) {
          setState(() {
            _uploadProgress = snapshot.bytesTransferred / snapshot.totalBytes;
          });
        }
      });

      await task;

      if (widget.photoToTake == 'Before') {
        // Mark the job so a returning user is routed to the After screen.
        await _addBeforePhotoUploadedMarker(ref);
        if (!mounted) return;
        Navigator.push(
          context,
          MaterialPageRoute(builder: (context) => TestPage(widget.inlet)),
        );
      } else if (widget.photoToTake == 'After') {
        await _completeJob(ref);
      }
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Upload failed. Please try again.')),
      );
    } finally {
      if (mounted) setState(() => _isUploading = false);
    }
  }

  Future<void> _completeJob(ref) async {
    final database = ref.read(databaseProvider);
    await database.updateInlet(widget.inlet.referenceId, data: {'status': 'cleaned'});
    await database.updateJob(widget.inlet.jobId, data: {"finishedAt": Timestamp.now(), "status": "cleaned"});
    _showMyDialog();
  }

  Future<void> _addBeforePhotoUploadedMarker(ref) async {
    final database = ref.read(databaseProvider);
    await database.updateInlet(widget.inlet.referenceId, data: {"status": "cleaning-with-before"});
  }

  Future<void> _showMyDialog() async {
    return showDialog<void>(
      context: context,
      barrierDismissible: false, // user must tap button!
      builder: (BuildContext context) {
        return AlertDialog(
          title: const Text('Cleaning complete'),
          content: const SingleChildScrollView(
            child: ListBody(
              children: <Widget>[
                Text('Thank you for cleaning this inlet'),
                Text('Your points will be awarded shortly'),
              ],
            ),
          ),
          actions: <Widget>[
            TextButton(
              child: const Text('Continue'),
              onPressed: () {
                Navigator.pushNamedAndRemoveUntil(context, '/home', (route) => false);
              },
            ),
          ],
        );
      },
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('${widget.photoToTake} Cleaning Photo'),
      ),
      body: SafeArea(
        child: Center(
          child: Column(
            children: [
              Container(
                alignment: Alignment.center,
                width: double.infinity,
                height: 300,
                color: Colors.grey[300],
                child: _image != null
                    ? Image.file(_image!, fit: BoxFit.cover)
                    : const Align(
                        alignment: Alignment.center,
                        child: Text(
                          'Please select take a photo or choose an image from your photo gallery',
                          textAlign: TextAlign.center,
                        )),
              ),
              Container(
                margin: const EdgeInsets.symmetric(horizontal: 20.0),
                child: Row(
                  mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                  children: [
                    Expanded(
                      child: ElevatedButton(
                        onPressed: () async {
                          _openImagePicker(ImageSource.camera);
                        },
                        child: const Text('Take a picture'),
                      ),
                    ),
                    const SizedBox(width: 10),
                    Expanded(
                      child: ElevatedButton(
                        onPressed: () async {
                          _openImagePicker(ImageSource.gallery);
                        },
                        child: const Text('Choose an image'),
                      ),
                    ),
                  ],
                ),
              ),
              const Spacer(),
              if (_isUploading)
                Padding(
                  padding: const EdgeInsets.symmetric(horizontal: 20.0),
                  child: LinearProgressIndicator(value: _uploadProgress),
                ),
              ElevatedButton.icon(
                onPressed: (_image == null || _isUploading) ? null : _uploadPhoto,
                icon: _isUploading
                    ? const SizedBox(
                        width: 18,
                        height: 18,
                        child: CircularProgressIndicator(strokeWidth: 2),
                      )
                    : const Icon(Icons.check),
                label: Text(_isUploading ? "Uploading..." : "Complete"),
                style: ElevatedButton.styleFrom(minimumSize: const Size.fromHeight(40)),
              )
            ],
          ),
        ),
      ),
    );
  }
}
