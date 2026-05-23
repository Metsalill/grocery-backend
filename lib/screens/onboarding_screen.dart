import 'dart:math' as math;
import 'dart:ui' as ui;

import 'package:flutter/material.dart';
import 'package:lottie/lottie.dart';
import 'package:shared_preferences/shared_preferences.dart';

class OnboardingScreen extends StatefulWidget {
  const OnboardingScreen({super.key});

  @override
  State<OnboardingScreen> createState() => _OnboardingScreenState();
}

class _OnboardingScreenState extends State<OnboardingScreen> {
  static const Color _pink = Color(0xFFC94B7C);
  static const Color _beige = Color(0xFFF4F0EA);
  static const Color _dark = Color(0xFF1A1A1A);

  final PageController _pageController = PageController();
  int _currentIndex = 0;
  double _page = 0;
  bool _isFinishing = false;

  late final List<_OnboardingSlideData> _slides = const [
    _OnboardingSlideData(
      title: 'Lisa tooted korvi',
      body: 'Sirvi tooteid või otsi retsepte. Lisa kõik mida vajad ühte korvi.',
      animationUrl:
          'https://assets-v2.lottiefiles.com/a/c63598e0-1cc8-11ef-b651-87d0cbf880cf/PHbHpmSsg4.lottie',
      fallbackIcon: Icons.add_shopping_cart_rounded,
      accent: _pink,
      softAccent: Color(0xFFF7D6E4),
      tag: 'Korv',
    ),
    _OnboardingSlideData(
      title: 'Võrdleme kõigi poodidega',
      body:
          'Seivy kontrollib automaatselt Rimi, Selveri, Prisma, Coopi ja Maxima hinnad sinu ümbruses.',
      animationUrl:
          'https://assets-v2.lottiefiles.com/a/88860ad6-e54e-11ee-a8e1-c707cdb69e95/lGzhoxAQWj.lottie',
      fallbackIcon: Icons.compare_arrows_rounded,
      accent: Color(0xFF2F8AB2),
      softAccent: Color(0xFFD7EDF6),
      tag: 'Võrdlus',
    ),
    _OnboardingSlideData(
      title: 'Leia odavaim ostukorv',
      body:
          'Näed koheselt kust tuleb kogu korv kõige soodsamalt - mitte ühe toote hind, vaid kogusumma.',
      animationUrl:
          'https://assets-v2.lottiefiles.com/a/61973c1e-116a-11ee-bc94-37a228360b3b/CejsK8x000.lottie',
      fallbackIcon: Icons.savings_rounded,
      accent: Color(0xFF4FA34D),
      softAccent: Color(0xFFDFF2DA),
      tag: 'Sääst',
    ),
  ];

  bool get _isLastSlide => _currentIndex == _slides.length - 1;

  @override
  void initState() {
    super.initState();
    _pageController.addListener(_handlePageScroll);
  }

  void _handlePageScroll() {
    final nextPage = _pageController.page ?? _currentIndex.toDouble();
    if (nextPage == _page) return;

    setState(() => _page = nextPage);
  }

  Future<void> _finishOnboarding() async {
    if (_isFinishing) return;

    setState(() => _isFinishing = true);

    final prefs = await SharedPreferences.getInstance();
    await prefs.setBool('onboarding_done', true);

    if (!mounted) return;
    Navigator.of(context).pushReplacementNamed('/home');
  }

  Future<void> _handlePrimaryAction() async {
    if (_isLastSlide) {
      await _finishOnboarding();
      return;
    }

    await _pageController.nextPage(
      duration: const Duration(milliseconds: 560),
      curve: Curves.easeOutCubic,
    );
  }

  @override
  void dispose() {
    _pageController
      ..removeListener(_handlePageScroll)
      ..dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: _beige,
      body: SafeArea(
        bottom: false,
        child: Stack(
          children: [
            Positioned.fill(
              child: DecoratedBox(
                decoration: BoxDecoration(
                  gradient: LinearGradient(
                    begin: Alignment.topCenter,
                    end: Alignment.bottomCenter,
                    colors: [
                      Colors.white.withOpacity(0.48),
                      _beige,
                      _beige,
                    ],
                  ),
                ),
              ),
            ),
            Positioned(
              top: -80,
              left: -70,
              child: _BlurredBlob(
                size: 230,
                color: _pink.withOpacity(0.13),
              ),
            ),
            Positioned(
              top: 110,
              right: -90,
              child: _BlurredBlob(
                size: 210,
                color: _slides[_currentIndex].accent.withOpacity(0.13),
              ),
            ),
            Column(
              children: [
                Expanded(
                  flex: 55,
                  child: PageView.builder(
                    controller: _pageController,
                    itemCount: _slides.length,
                    onPageChanged: (index) {
                      setState(() {
                        _currentIndex = index;
                        _page = index.toDouble();
                      });
                    },
                    itemBuilder: (context, index) {
                      return _AnimatedIllustrationPage(
                        page: _page,
                        index: index,
                        slide: _slides[index],
                        dark: _dark,
                      );
                    },
                  ),
                ),
                Expanded(
                  flex: 45,
                  child: _BottomContent(
                    slide: _slides[_currentIndex],
                    slides: _slides,
                    page: _page,
                    currentIndex: _currentIndex,
                    isLastSlide: _isLastSlide,
                    isFinishing: _isFinishing,
                    onPrimaryAction: _handlePrimaryAction,
                    dark: _dark,
                    pink: _pink,
                  ),
                ),
              ],
            ),
            Positioned(
              top: 10,
              right: 18,
              child: _SkipButton(
                onPressed: _finishOnboarding,
                isLoading: _isFinishing,
                dark: _dark,
              ),
            ),
            Positioned(
              top: 12,
              left: 18,
              child: _BrandMark(pink: _pink, dark: _dark),
            ),
          ],
        ),
      ),
    );
  }
}

class _AnimatedIllustrationPage extends StatelessWidget {
  const _AnimatedIllustrationPage({
    required this.page,
    required this.index,
    required this.slide,
    required this.dark,
  });

  final double page;
  final int index;
  final _OnboardingSlideData slide;
  final Color dark;

  @override
  Widget build(BuildContext context) {
    final distance = (page - index).clamp(-1.0, 1.0).toDouble();
    final progress = distance.abs();
    final scale = ui.lerpDouble(1, 0.9, progress)!;
    final yOffset = ui.lerpDouble(0, 26, progress)!;
    final rotation = ui.lerpDouble(0, distance * -0.05, progress)!;

    return Transform.translate(
      offset: Offset(distance * -42, yOffset),
      child: Transform.rotate(
        angle: rotation,
        child: Transform.scale(
          scale: scale,
          child: Opacity(
            opacity: ui.lerpDouble(1, 0.58, progress)!,
            child: Padding(
              padding: const EdgeInsets.fromLTRB(24, 64, 24, 10),
              child: _IllustrationCard(slide: slide, dark: dark),
            ),
          ),
        ),
      ),
    );
  }
}

class _IllustrationCard extends StatelessWidget {
  const _IllustrationCard({
    required this.slide,
    required this.dark,
  });

  final _OnboardingSlideData slide;
  final Color dark;

  @override
  Widget build(BuildContext context) {
    return Stack(
      alignment: Alignment.center,
      children: [
        Positioned.fill(
          child: DecoratedBox(
            decoration: BoxDecoration(
              borderRadius: BorderRadius.circular(38),
              boxShadow: [
                BoxShadow(
                  color: slide.accent.withOpacity(0.20),
                  blurRadius: 34,
                  offset: const Offset(0, 22),
                ),
              ],
              gradient: LinearGradient(
                begin: Alignment.topLeft,
                end: Alignment.bottomRight,
                colors: [
                  Colors.white,
                  slide.softAccent,
                ],
              ),
            ),
          ),
        ),
        Positioned(
          top: 22,
          left: 22,
          child: _SlideTag(slide: slide),
        ),
        Positioned(
          right: 20,
          bottom: 20,
          child: _PriceBubble(accent: slide.accent),
        ),
        Positioned(
          left: 22,
          bottom: 26,
          child: _StoreBadges(accent: slide.accent),
        ),
        Positioned.fill(
          child: Padding(
            padding: const EdgeInsets.fromLTRB(28, 30, 28, 38),
            child: Lottie.network(
              slide.animationUrl,
              fit: BoxFit.contain,
              repeat: true,
              animate: true,
              frameRate: FrameRate.max,
              errorBuilder: (context, error, stackTrace) {
                return _FallbackAnimation(slide: slide);
              },
            ),
          ),
        ),
      ],
    );
  }
}

class _BottomContent extends StatelessWidget {
  const _BottomContent({
    required this.slide,
    required this.slides,
    required this.page,
    required this.currentIndex,
    required this.isLastSlide,
    required this.isFinishing,
    required this.onPrimaryAction,
    required this.dark,
    required this.pink,
  });

  final _OnboardingSlideData slide;
  final List<_OnboardingSlideData> slides;
  final double page;
  final int currentIndex;
  final bool isLastSlide;
  final bool isFinishing;
  final Future<void> Function() onPrimaryAction;
  final Color dark;
  final Color pink;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.fromLTRB(24, 22, 24, 26),
      decoration: const BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.vertical(top: Radius.circular(34)),
        boxShadow: [
          BoxShadow(
            color: Color(0x14000000),
            blurRadius: 28,
            offset: Offset(0, -12),
          ),
        ],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          _JourneyIndicator(
            slides: slides,
            page: page,
            currentIndex: currentIndex,
            dark: dark,
          ),
          const SizedBox(height: 24),
          AnimatedSwitcher(
            duration: const Duration(milliseconds: 360),
            switchInCurve: Curves.easeOutCubic,
            switchOutCurve: Curves.easeInCubic,
            transitionBuilder: (child, animation) {
              final slideAnimation = Tween<Offset>(
                begin: const Offset(0.08, 0),
                end: Offset.zero,
              ).animate(animation);

              return FadeTransition(
                opacity: animation,
                child: SlideTransition(position: slideAnimation, child: child),
              );
            },
            child: Column(
              key: ValueKey(slide.title),
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  slide.title,
                  style: TextStyle(
                    color: dark,
                    fontSize: 31,
                    height: 1.04,
                    fontWeight: FontWeight.w900,
                    letterSpacing: -0.9,
                  ),
                ),
                const SizedBox(height: 14),
                Text(
                  slide.body,
                  style: TextStyle(
                    color: dark.withOpacity(0.68),
                    fontSize: 16.5,
                    height: 1.45,
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ],
            ),
          ),
          const Spacer(),
          SizedBox(
            width: double.infinity,
            height: 60,
            child: FilledButton(
              onPressed: isFinishing ? null : onPrimaryAction,
              style: FilledButton.styleFrom(
                backgroundColor: isLastSlide ? pink : dark,
                disabledBackgroundColor: pink.withOpacity(0.42),
                foregroundColor: Colors.white,
                elevation: 0,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(22),
                ),
              ),
              child: AnimatedSwitcher(
                duration: const Duration(milliseconds: 220),
                child: isFinishing
                    ? const SizedBox(
                        key: ValueKey('loading'),
                        height: 22,
                        width: 22,
                        child: CircularProgressIndicator(
                          strokeWidth: 2.6,
                          valueColor: AlwaysStoppedAnimation<Color>(
                            Colors.white,
                          ),
                        ),
                      )
                    : Row(
                        key: ValueKey(isLastSlide),
                        mainAxisAlignment: MainAxisAlignment.center,
                        mainAxisSize: MainAxisSize.min,
                        children: [
                          Text(
                            isLastSlide ? 'Alusta' : 'Järgmine',
                            style: const TextStyle(
                              fontSize: 17,
                              fontWeight: FontWeight.w900,
                              letterSpacing: 0.1,
                            ),
                          ),
                          const SizedBox(width: 10),
                          Icon(
                            isLastSlide
                                ? Icons.shopping_basket_rounded
                                : Icons.arrow_forward_rounded,
                            size: 22,
                          ),
                        ],
                      ),
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _JourneyIndicator extends StatelessWidget {
  const _JourneyIndicator({
    required this.slides,
    required this.page,
    required this.currentIndex,
    required this.dark,
  });

  final List<_OnboardingSlideData> slides;
  final double page;
  final int currentIndex;
  final Color dark;

  @override
  Widget build(BuildContext context) {
    final progress = (page / (slides.length - 1)).clamp(0.0, 1.0).toDouble();

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        LayoutBuilder(
          builder: (context, constraints) {
            return Stack(
              alignment: Alignment.centerLeft,
              children: [
                Container(
                  height: 8,
                  decoration: BoxDecoration(
                    color: const Color(0xFFEDE4DA),
                    borderRadius: BorderRadius.circular(999),
                  ),
                ),
                AnimatedContainer(
                  duration: const Duration(milliseconds: 240),
                  curve: Curves.easeOut,
                  height: 8,
                  width: math.max(28, constraints.maxWidth * progress),
                  decoration: BoxDecoration(
                    borderRadius: BorderRadius.circular(999),
                    gradient: LinearGradient(
                      colors: [
                        slides.first.accent,
                        slides[currentIndex].accent,
                      ],
                    ),
                  ),
                ),
                Positioned(
                  left: (constraints.maxWidth - 34) * progress,
                  child: Container(
                    width: 34,
                    height: 34,
                    decoration: BoxDecoration(
                      color: Colors.white,
                      shape: BoxShape.circle,
                      border: Border.all(
                        color: slides[currentIndex].accent,
                        width: 3,
                      ),
                      boxShadow: [
                        BoxShadow(
                          color: slides[currentIndex].accent.withOpacity(0.28),
                          blurRadius: 18,
                          offset: const Offset(0, 8),
                        ),
                      ],
                    ),
                    child: Icon(
                      Icons.shopping_basket_rounded,
                      color: slides[currentIndex].accent,
                      size: 18,
                    ),
                  ),
                ),
              ],
            );
          },
        ),
        const SizedBox(height: 16),
        Row(
          children: [
            for (var i = 0; i < slides.length; i++) ...[
              Expanded(
                child: _MorphingStepPill(
                  slide: slides[i],
                  isActive: i == currentIndex,
                  index: i,
                  dark: dark,
                ),
              ),
              if (i != slides.length - 1) const SizedBox(width: 8),
            ],
          ],
        ),
      ],
    );
  }
}

class _MorphingStepPill extends StatelessWidget {
  const _MorphingStepPill({
    required this.slide,
    required this.isActive,
    required this.index,
    required this.dark,
  });

  final _OnboardingSlideData slide;
  final bool isActive;
  final int index;
  final Color dark;

  @override
  Widget build(BuildContext context) {
    return AnimatedContainer(
      duration: const Duration(milliseconds: 300),
      curve: Curves.easeOutCubic,
      height: 44,
      padding: EdgeInsets.symmetric(horizontal: isActive ? 12 : 8),
      decoration: BoxDecoration(
        color: isActive ? slide.accent.withOpacity(0.12) : const Color(0xFFF7F3EE),
        borderRadius: BorderRadius.circular(isActive ? 16 : 24),
        border: Border.all(
          color: isActive ? slide.accent.withOpacity(0.36) : Colors.transparent,
        ),
      ),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.center,
        mainAxisSize: MainAxisSize.min,
        children: [
          AnimatedContainer(
            duration: const Duration(milliseconds: 300),
            width: isActive ? 25 : 22,
            height: isActive ? 25 : 22,
            decoration: BoxDecoration(
              color: isActive ? slide.accent : Colors.white,
              borderRadius: BorderRadius.circular(isActive ? 9 : 14),
            ),
            child: Center(
              child: Text(
                '${index + 1}',
                style: TextStyle(
                  color: isActive ? Colors.white : dark.withOpacity(0.42),
                  fontSize: 12,
                  fontWeight: FontWeight.w900,
                ),
              ),
            ),
          ),
          AnimatedSize(
            duration: const Duration(milliseconds: 240),
            curve: Curves.easeOutCubic,
            child: isActive
                ? Padding(
                    padding: const EdgeInsets.only(left: 8),
                    child: Text(
                      slide.tag,
                      maxLines: 1,
                      overflow: TextOverflow.fade,
                      softWrap: false,
                      style: TextStyle(
                        color: slide.accent,
                        fontSize: 13,
                        fontWeight: FontWeight.w900,
                      ),
                    ),
                  )
                : const SizedBox.shrink(),
          ),
        ],
      ),
    );
  }
}

class _FallbackAnimation extends StatelessWidget {
  const _FallbackAnimation({required this.slide});

  final _OnboardingSlideData slide;

  @override
  Widget build(BuildContext context) {
    return Center(
      child: TweenAnimationBuilder<double>(
        tween: Tween(begin: 0.92, end: 1),
        duration: const Duration(milliseconds: 900),
        curve: Curves.elasticOut,
        builder: (context, value, child) {
          return Transform.scale(scale: value, child: child);
        },
        child: Container(
          width: 170,
          height: 170,
          decoration: BoxDecoration(
            shape: BoxShape.circle,
            gradient: LinearGradient(
              begin: Alignment.topLeft,
              end: Alignment.bottomRight,
              colors: [
                slide.accent.withOpacity(0.92),
                slide.accent.withOpacity(0.68),
              ],
            ),
            boxShadow: [
              BoxShadow(
                color: slide.accent.withOpacity(0.25),
                blurRadius: 32,
                offset: const Offset(0, 18),
              ),
            ],
          ),
          child: Icon(
            slide.fallbackIcon,
            color: Colors.white,
            size: 76,
          ),
        ),
      ),
    );
  }
}

class _BrandMark extends StatelessWidget {
  const _BrandMark({
    required this.pink,
    required this.dark,
  });

  final Color pink;
  final Color dark;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 13, vertical: 9),
      decoration: BoxDecoration(
        color: Colors.white.withOpacity(0.82),
        borderRadius: BorderRadius.circular(18),
        border: Border.all(color: Colors.white),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Container(
            width: 26,
            height: 26,
            decoration: BoxDecoration(
              color: pink,
              borderRadius: BorderRadius.circular(9),
            ),
            child: const Icon(
              Icons.bar_chart_rounded,
              color: Colors.white,
              size: 17,
            ),
          ),
          const SizedBox(width: 8),
          Text(
            'Seivy',
            style: TextStyle(
              color: dark,
              fontSize: 17,
              fontWeight: FontWeight.w900,
              letterSpacing: -0.3,
            ),
          ),
        ],
      ),
    );
  }
}

class _SkipButton extends StatelessWidget {
  const _SkipButton({
    required this.onPressed,
    required this.isLoading,
    required this.dark,
  });

  final VoidCallback onPressed;
  final bool isLoading;
  final Color dark;

  @override
  Widget build(BuildContext context) {
    return TextButton(
      onPressed: isLoading ? null : onPressed,
      style: TextButton.styleFrom(
        foregroundColor: dark,
        backgroundColor: Colors.white.withOpacity(0.82),
        padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 10),
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(18),
        ),
      ),
      child: Text(
        'Jäta vahele',
        style: TextStyle(
          color: dark.withOpacity(0.72),
          fontWeight: FontWeight.w900,
        ),
      ),
    );
  }
}

class _SlideTag extends StatelessWidget {
  const _SlideTag({required this.slide});

  final _OnboardingSlideData slide;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      decoration: BoxDecoration(
        color: Colors.white.withOpacity(0.88),
        borderRadius: BorderRadius.circular(999),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(slide.fallbackIcon, size: 17, color: slide.accent),
          const SizedBox(width: 7),
          Text(
            slide.tag,
            style: TextStyle(
              color: slide.accent,
              fontWeight: FontWeight.w900,
              fontSize: 13,
            ),
          ),
        ],
      ),
    );
  }
}

class _PriceBubble extends StatelessWidget {
  const _PriceBubble({required this.accent});

  final Color accent;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 13, vertical: 10),
      decoration: BoxDecoration(
        color: Colors.white.withOpacity(0.94),
        borderRadius: BorderRadius.circular(18),
        boxShadow: const [
          BoxShadow(
            color: Color(0x12000000),
            blurRadius: 16,
            offset: Offset(0, 8),
          ),
        ],
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(Icons.auto_awesome_rounded, size: 17, color: accent),
          const SizedBox(width: 6),
          Text(
            '-18%',
            style: TextStyle(
              color: accent,
              fontWeight: FontWeight.w900,
              fontSize: 15,
            ),
          ),
        ],
      ),
    );
  }
}

class _StoreBadges extends StatelessWidget {
  const _StoreBadges({required this.accent});

  final Color accent;

  static const List<String> _stores = ['Rimi', 'Selver', 'Prisma', 'Coop', 'Maxima'];

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      width: 148,
      child: Wrap(
        spacing: 6,
        runSpacing: 6,
        children: [
          for (final store in _stores)
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 5),
              decoration: BoxDecoration(
                color: Colors.white.withOpacity(0.86),
                borderRadius: BorderRadius.circular(999),
              ),
              child: Text(
                store,
                style: TextStyle(
                  color: accent,
                  fontSize: 10.5,
                  fontWeight: FontWeight.w900,
                ),
              ),
            ),
        ],
      ),
    );
  }
}

class _BlurredBlob extends StatelessWidget {
  const _BlurredBlob({
    required this.size,
    required this.color,
  });

  final double size;
  final Color color;

  @override
  Widget build(BuildContext context) {
    return ImageFiltered(
      imageFilter: ui.ImageFilter.blur(sigmaX: 42, sigmaY: 42),
      child: Container(
        width: size,
        height: size,
        decoration: BoxDecoration(
          color: color,
          shape: BoxShape.circle,
        ),
      ),
    );
  }
}

class _OnboardingSlideData {
  const _OnboardingSlideData({
    required this.title,
    required this.body,
    required this.animationUrl,
    required this.fallbackIcon,
    required this.accent,
    required this.softAccent,
    required this.tag,
  });

  final String title;
  final String body;
  final String animationUrl;
  final IconData fallbackIcon;
  final Color accent;
  final Color softAccent;
  final String tag;
}
