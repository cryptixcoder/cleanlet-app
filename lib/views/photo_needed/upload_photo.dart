import 'package:cleanlet/services/firestore_repository.dart';
import 'package:firebase_storage/firebase_storage.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:image_picker/image_picker.dart';
import 'dart:io';

import '../../models/inlet.dart';

class UploadPhoto extends ConsumerStatefulWidget {
  final Inlet inlet;
  const UploadPhoto(this.inlet, {super.key});

  @override
  ConsumerState<UploadPhoto> createState() => _UploadPhotoState();
}

class _UploadPhotoState extends ConsumerState<UploadPhoto> {
  File? _image;
  final _picker = ImagePicker();
  final storageRef = FirebaseStorage.instance.ref();

  final _formKey = GlobalKey<FormState>();
  final _addressController = TextEditingController();
  final _descriptionController = TextEditingController();

  bool _isUploading = false;
  double _uploadProgress = 0;

  // Inlet photos are shown in a small carousel, so a moderate downscale keeps
  // uploads fast with no visible quality loss.
  static const _maxImageWidth = 1920.0;
  static const _imageQuality = 80;

  @override
  void dispose() {
    _addressController.dispose();
    _descriptionController.dispose();
    super.dispose();
  }

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
    if (!(_formKey.currentState?.validate() ?? false) || _image == null) return;

    setState(() {
      _isUploading = true;
      _uploadProgress = 0;
    });

    try {
      final filename = '${widget.inlet.referenceId}.jpg';
      final uploadRef = storageRef.child('inlet-photos').child(filename);

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

      final database = ref.read(databaseProvider);
      await database.updateInlet(widget.inlet.referenceId, data: {
        "images": [filename],
        "inletStatus": "review",
        "address": _addressController.text.trim(),
        "description": _descriptionController.text.trim(),
      });

      if (!mounted) return;
      await _showMyDialog();
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Upload failed. Please try again.')),
      );
    } finally {
      if (mounted) setState(() => _isUploading = false);
    }
  }

  Future<void> _showMyDialog() async {
    return showDialog<void>(
        context: context,
        barrierDismissible: false,
        builder: (BuildContext context) {
          return AlertDialog(
              title: const Text('Photo Uploaded'),
              content: const SingleChildScrollView(
                  child: ListBody(
                children: <Widget>[
                  Text('Thank you for uploading a photo'),
                  Text('Admins will review your photos'),
                ],
              )),
              actions: <Widget>[
                TextButton(
                  child: const Text('Return to Home'),
                  onPressed: () {
                    Navigator.pushNamedAndRemoveUntil(context, '/home', (route) => false);
                  },
                )
              ]);
        });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
        appBar: AppBar(title: Text('Upload Photo')),
        body: SafeArea(
            child: Form(
                key: _formKey,
                child: LayoutBuilder(
                    builder: (context, constraints) => SingleChildScrollView(
                        padding: EdgeInsets.only(
                          bottom: MediaQuery.of(context).viewInsets.bottom,
                        ),
                        child: ConstrainedBox(
                            constraints: BoxConstraints(minHeight: constraints.maxHeight),
                            child: IntrinsicHeight(
                                child: Column(
                              children: [
                                Container(alignment: Alignment.center, width: double.infinity, height: 261, color: Colors.grey[300], child: _image != null ? Image.file(_image!, fit: BoxFit.cover) : const Align(alignment: Alignment.center, child: Text('Please select take a photo or choose an image from your photo gallery', textAlign: TextAlign.center))),
                                Container(
                                    margin: const EdgeInsets.symmetric(horizontal: 20.0, vertical: 20.0),
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
                                    )),
                                Padding(
                                    padding: const EdgeInsets.symmetric(horizontal: 20.0),
                                    child: TextFormField(
                                      controller: _addressController,
                                      decoration: const InputDecoration(
                                        labelText: 'Address (Required)',
                                        border: OutlineInputBorder(),
                                      ),
                                      validator: (value) {
                                        if (value == null || value.trim().isEmpty) {
                                          return 'Address is required';
                                        }
                                        return null;
                                      },
                                    )),
                                const SizedBox(height: 20),
                                Padding(
                                    padding: const EdgeInsets.symmetric(
                                      horizontal: 20.0,
                                    ),
                                    child: TextFormField(
                                        controller: _descriptionController,
                                        decoration: const InputDecoration(
                                          labelText: 'Description (optional)',
                                          border: OutlineInputBorder(),
                                        ),
                                        maxLines: 3, // allows unlimited lines
                                        keyboardType: TextInputType.multiline, // ensures multiline keyboard
                                        textInputAction: TextInputAction.done, // shows Done button
                                        onFieldSubmitted: (_) {
                                          FocusScope.of(context).unfocus(); // dismisses the keyboard
                                        })),
                                const Spacer(),
                                if (_isUploading)
                                  Padding(
                                    padding: const EdgeInsets.symmetric(horizontal: 10.0),
                                    child: LinearProgressIndicator(value: _uploadProgress),
                                  ),
                                Container(
                                    margin: const EdgeInsets.symmetric(horizontal: 10.0),
                                    padding: const EdgeInsets.only(top: 20.0),
                                    child: ElevatedButton.icon(
                                      onPressed: (_image == null || _isUploading) ? null : _uploadPhoto,
                                      icon: _isUploading
                                          ? const SizedBox(
                                              width: 18,
                                              height: 18,
                                              child: CircularProgressIndicator(strokeWidth: 2),
                                            )
                                          : const Icon(Icons.check),
                                      label: Text(_isUploading ? "Uploading..." : "Upload Photo"),
                                      style: ElevatedButton.styleFrom(minimumSize: const Size.fromHeight(40)),
                                    )),
                              ],
                            ))))))));
  }
}
