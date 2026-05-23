import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';

class OnboardingScreen extends StatefulWidget {
  const OnboardingScreen({super.key});

  @override
  State<OnboardingScreen> createState() => _OnboardingScreenState();
}

class _OnboardingScreenState extends State<OnboardingScreen> {
  final PageController _controller = PageController();
  int _index = 0;
  bool _finishing = false;

  static const List<String> _slideAssets = [
    'assets/onboarding/onboarding_clean_slide_1.png',
    'assets/onboarding/onboarding_clean_slide_2.png',
    'assets/onboarding/onboarding_clean_slide_3.png',
  ];

  bool get _isLast => _index == _slideAssets.length - 1;

  Future<void> _finish() async {
    if (_finishing) return;

    setState(() => _finishing = true);
    final prefs = await SharedPreferences.getInstance();
    await prefs.setBool('onboarding_done', true);

    if (!mounted) return;
    Navigator.of(context).pushReplacementNamed('/home');
  }

  Future<void> _next() async {
    if (_isLast) {
      await _finish();
      return;
    }

    await _controller.nextPage(
      duration: const Duration(milliseconds: 420),
      curve: Curves.easeOutCubic,
    );
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFFF4F0EA),
      body: SafeArea(
        bottom: false,
        child: Stack(
          children: [
            PageView.builder(
              controller: _controller,
              itemCount: _slideAssets.length,
              onPageChanged: (value) => setState(() => _index = value),
              itemBuilder: (context, index) {
                return Image.asset(
                  _slideAssets[index],
                  width: double.infinity,
                  height: double.infinity,
                  fit: BoxFit.fill,
                  filterQuality: FilterQuality.high,
                );
              },
            ),
            Positioned(
              top: 0,
              right: 0,
              width: 170,
              height: 82,
              child: Material(
                color: Colors.transparent,
                child: InkWell(
                  onTap: _finishing ? null : _finish,
                  splashColor: Colors.transparent,
                  highlightColor: Colors.transparent,
                ),
              ),
            ),
            Positioned(
              left: 16,
              right: 16,
              bottom: 0,
              height: 92,
              child: Material(
                color: Colors.transparent,
                child: InkWell(
                  onTap: _finishing ? null : _next,
                  splashColor: Colors.transparent,
                  highlightColor: Colors.transparent,
                ),
              ),
            ),
            if (_finishing)
              const Positioned.fill(
                child: ColoredBox(
                  color: Color(0x33000000),
                  child: Center(
                    child: CircularProgressIndicator(
                      color: Color(0xFFC94B7C),
                    ),
                  ),
                ),
              ),
          ],
        ),
      ),
    );
  }
}
