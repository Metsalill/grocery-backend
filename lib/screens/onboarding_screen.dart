import 'dart:ui' as ui;

import 'package:flutter/material.dart';
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

  final PageController _controller = PageController();
  double _page = 0;
  int _index = 0;
  bool _finishing = false;

  static const List<_SlideData> _slides = [
    _SlideData(
      image: 'assets/images/onboarding1.png',
      label: 'Pasta',
      title: 'Lisa tooted korvi',
      body: 'Sirvi tooteid või otsi retsepte. Lisa kõik mida vajad ühte korvi.',
      icon: Icons.add_rounded,
    ),
    _SlideData(
      image: 'assets/images/onboarding2.png',
      label: 'Võrdlus',
      title: 'Võrdleme kõigi poodidega',
      body:
          'Seivy kontrollib automaatselt Rimi, Selveri, Prisma, Coopi ja Maxima hinnad sinu ümbruses.',
      icon: Icons.search_rounded,
    ),
    _SlideData(
      image: 'assets/images/onboarding3.png',
      label: 'Sääst',
      title: 'Leia odavaim ostukorv',
      body:
          'Näed koheselt kust tuleb kogu korv kõige soodsamalt - mitte ühe toote hind, vaid kogusumma.',
      icon: Icons.euro_rounded,
    ),
  ];

  bool get _isLast => _index == _slides.length - 1;

  @override
  void initState() {
    super.initState();
    _controller.addListener(_onScroll);
  }

  void _onScroll() {
    final nextPage = _controller.page ?? _index.toDouble();
    if (nextPage == _page) return;
    setState(() => _page = nextPage);
  }

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
      duration: const Duration(milliseconds: 520),
      curve: Curves.easeOutCubic,
    );
  }

  @override
  void dispose() {
    _controller
      ..removeListener(_onScroll)
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
            const _SoftBackground(),
            Positioned(
              top: 16,
              left: 18,
              child: _BrandPill(pink: _pink, dark: _dark),
            ),
            Positioned(
              top: 16,
              right: 18,
              child: _SkipPill(
                enabled: !_finishing,
                onTap: _finish,
                dark: _dark,
              ),
            ),
            Column(
              children: [
                Expanded(
                  flex: 58,
                  child: PageView.builder(
                    controller: _controller,
                    itemCount: _slides.length,
                    onPageChanged: (value) {
                      setState(() {
                        _index = value;
                        _page = value.toDouble();
                      });
                    },
                    itemBuilder: (context, index) {
                      return _HeroSlide(
                        slide: _slides[index],
                        index: index,
                        page: _page,
                      );
                    },
                  ),
                ),
                Expanded(
                  flex: 42,
                  child: _BottomPanel(
                    slide: _slides[_index],
                    page: _page,
                    isLast: _isLast,
                    finishing: _finishing,
                    onTap: _next,
                    pink: _pink,
                    dark: _dark,
                  ),
                ),
              ],
            ),
            if (_finishing)
              const Positioned.fill(
                child: ColoredBox(
                  color: Color(0x33000000),
                  child: Center(
                    child: CircularProgressIndicator(color: _pink),
                  ),
                ),
              ),
          ],
        ),
      ),
    );
  }
}

class _HeroSlide extends StatelessWidget {
  const _HeroSlide({
    required this.slide,
    required this.index,
    required this.page,
  });

  final _SlideData slide;
  final int index;
  final double page;

  @override
  Widget build(BuildContext context) {
    final distance = (page - index).clamp(-1.0, 1.0).toDouble();
    final hidden = distance.abs();

    return Transform.translate(
      offset: Offset(distance * -34, ui.lerpDouble(0, 18, hidden)!),
      child: Transform.scale(
        scale: ui.lerpDouble(1, 0.9, hidden)!,
        child: Opacity(
          opacity: ui.lerpDouble(1, 0.62, hidden)!,
          child: Padding(
            padding: const EdgeInsets.fromLTRB(22, 84, 22, 12),
            child: _IllustrationCard(slide: slide),
          ),
        ),
      ),
    );
  }
}

class _IllustrationCard extends StatelessWidget {
  const _IllustrationCard({required this.slide});

  final _SlideData slide;

  @override
  Widget build(BuildContext context) {
    return Container(
      clipBehavior: Clip.antiAlias,
      decoration: BoxDecoration(
        color: const Color(0xFFF8EADF),
        borderRadius: BorderRadius.circular(34),
        boxShadow: [
          BoxShadow(
            color: const Color(0xFF8C5C42).withValues(alpha: 0.11),
            blurRadius: 28,
            offset: const Offset(0, 18),
          ),
        ],
      ),
      child: Stack(
        children: [
          Positioned.fill(child: CustomPaint(painter: _CardPatternPainter())),
          Positioned(
            top: 18,
            left: 18,
            child: _MiniLabel(slide: slide),
          ),
          Positioned.fill(
            child: Padding(
              padding: const EdgeInsets.fromLTRB(6, 34, 6, 0),
              child: Image.asset(
                slide.image,
                fit: BoxFit.contain,
                filterQuality: FilterQuality.high,
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _BottomPanel extends StatelessWidget {
  const _BottomPanel({
    required this.slide,
    required this.page,
    required this.isLast,
    required this.finishing,
    required this.onTap,
    required this.pink,
    required this.dark,
  });

  final _SlideData slide;
  final double page;
  final bool isLast;
  final bool finishing;
  final VoidCallback onTap;
  final Color pink;
  final Color dark;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.fromLTRB(26, 24, 26, 24),
      decoration: const BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.vertical(top: Radius.circular(34)),
      ),
      child: Column(
        children: [
          _CreativeIndicator(page: page, pink: pink),
          const SizedBox(height: 28),
          AnimatedSwitcher(
            duration: const Duration(milliseconds: 280),
            child: Column(
              key: ValueKey(slide.title),
              children: [
                Text(
                  slide.title,
                  textAlign: TextAlign.center,
                  style: TextStyle(
                    color: dark,
                    fontSize: 25,
                    height: 1.08,
                    fontWeight: FontWeight.w900,
                    letterSpacing: -0.8,
                  ),
                ),
                const SizedBox(height: 14),
                Text(
                  slide.body,
                  textAlign: TextAlign.center,
                  style: TextStyle(
                    color: dark.withValues(alpha: 0.68),
                    fontSize: 15.5,
                    height: 1.48,
                    fontWeight: FontWeight.w500,
                  ),
                ),
              ],
            ),
          ),
          const Spacer(),
          SizedBox(
            width: double.infinity,
            height: 58,
            child: FilledButton(
              onPressed: finishing ? null : onTap,
              style: FilledButton.styleFrom(
                backgroundColor: isLast ? pink : Colors.black,
                disabledBackgroundColor: pink.withValues(alpha: 0.42),
                foregroundColor: Colors.white,
                elevation: 0,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(18),
                ),
              ),
              child: Row(
                mainAxisAlignment: MainAxisAlignment.center,
                mainAxisSize: MainAxisSize.min,
                children: [
                  Text(
                    isLast ? 'Alusta' : 'Järgmine',
                    style: const TextStyle(
                      fontSize: 17,
                      fontWeight: FontWeight.w900,
                    ),
                  ),
                  const SizedBox(width: 12),
                  Icon(
                    isLast
                        ? Icons.shopping_basket_rounded
                        : Icons.arrow_forward_rounded,
                    size: 22,
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _CreativeIndicator extends StatelessWidget {
  const _CreativeIndicator({
    required this.page,
    required this.pink,
  });

  final double page;
  final Color pink;

  @override
  Widget build(BuildContext context) {
    final active = page.round().clamp(0, 2).toInt();

    return SizedBox(
      height: 48,
      child: CustomPaint(
        painter: _IndicatorLinePainter(page: page, pink: pink),
        child: Row(
          children: List.generate(3, (index) {
            final done = index < active;
            final selected = index == active;
            return Expanded(
              child: Center(
                child: AnimatedContainer(
                  duration: const Duration(milliseconds: 260),
                  curve: Curves.easeOutCubic,
                  width: selected ? 82 : 44,
                  height: 44,
                  decoration: BoxDecoration(
                    color: done || selected
                        ? pink
                        : const Color(0xFFF1ECE5),
                    borderRadius: BorderRadius.circular(999),
                    boxShadow: selected
                        ? [
                            BoxShadow(
                              color: pink.withValues(alpha: 0.25),
                              blurRadius: 16,
                              offset: const Offset(0, 8),
                            ),
                          ]
                        : null,
                  ),
                  child: Center(
                    child: done
                        ? const Icon(
                            Icons.check_rounded,
                            color: Colors.white,
                            size: 21,
                          )
                        : Text(
                            '${index + 1}',
                            style: TextStyle(
                              color: selected
                                  ? Colors.white
                                  : const Color(0xFFB9B0A7),
                              fontSize: 17,
                              fontWeight: FontWeight.w900,
                            ),
                          ),
                  ),
                ),
              ),
            );
          }),
        ),
      ),
    );
  }
}

class _IndicatorLinePainter extends CustomPainter {
  const _IndicatorLinePainter({
    required this.page,
    required this.pink,
  });

  final double page;
  final Color pink;

  @override
  void paint(Canvas canvas, Size size) {
    final y = size.height / 2;
    final start = Offset(size.width / 6, y);
    final end = Offset(size.width * 5 / 6, y);

    final track = Paint()
      ..color = const Color(0xFFE7DFD6)
      ..strokeWidth = 4
      ..strokeCap = StrokeCap.round;
    final active = Paint()
      ..color = pink
      ..strokeWidth = 5
      ..strokeCap = StrokeCap.round;

    canvas.drawLine(start, end, track);
    final progress = (page / 2).clamp(0.0, 1.0).toDouble();
    canvas.drawLine(start, Offset.lerp(start, end, progress)!, active);
  }

  @override
  bool shouldRepaint(covariant _IndicatorLinePainter oldDelegate) {
    return oldDelegate.page != page || oldDelegate.pink != pink;
  }
}

class _BrandPill extends StatelessWidget {
  const _BrandPill({
    required this.pink,
    required this.dark,
  });

  final Color pink;
  final Color dark;

  @override
  Widget build(BuildContext context) {
    return Container(
      height: 48,
      padding: const EdgeInsets.symmetric(horizontal: 12),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(17),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withValues(alpha: 0.04),
            blurRadius: 16,
            offset: const Offset(0, 8),
          ),
        ],
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Container(
            width: 29,
            height: 29,
            decoration: BoxDecoration(
              color: pink,
              borderRadius: BorderRadius.circular(9),
            ),
            child: const Icon(
              Icons.bar_chart_rounded,
              color: Colors.white,
              size: 19,
            ),
          ),
          const SizedBox(width: 8),
          Text(
            'Seivy',
            style: TextStyle(
              color: dark,
              fontSize: 17,
              fontWeight: FontWeight.w900,
              letterSpacing: -0.4,
            ),
          ),
        ],
      ),
    );
  }
}

class _SkipPill extends StatelessWidget {
  const _SkipPill({
    required this.enabled,
    required this.onTap,
    required this.dark,
  });

  final bool enabled;
  final VoidCallback onTap;
  final Color dark;

  @override
  Widget build(BuildContext context) {
    return Material(
      color: Colors.white,
      borderRadius: BorderRadius.circular(18),
      child: InkWell(
        onTap: enabled ? onTap : null,
        borderRadius: BorderRadius.circular(18),
        child: Padding(
          padding: const EdgeInsets.symmetric(horizontal: 17, vertical: 14),
          child: Text(
            'Jäta vahele',
            style: TextStyle(
              color: dark.withValues(alpha: 0.67),
              fontSize: 14,
              fontWeight: FontWeight.w900,
            ),
          ),
        ),
      ),
    );
  }
}

class _MiniLabel extends StatelessWidget {
  const _MiniLabel({required this.slide});

  final _SlideData slide;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(13),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(slide.icon, color: _OnboardingScreenState._pink, size: 16),
          const SizedBox(width: 6),
          Text(
            slide.label,
            style: const TextStyle(
              color: _OnboardingScreenState._pink,
              fontSize: 12,
              fontWeight: FontWeight.w900,
            ),
          ),
        ],
      ),
    );
  }
}

class _SoftBackground extends StatelessWidget {
  const _SoftBackground();

  @override
  Widget build(BuildContext context) {
    return const Positioned.fill(
      child: DecoratedBox(
        decoration: BoxDecoration(
          gradient: LinearGradient(
            begin: Alignment.topCenter,
            end: Alignment.bottomCenter,
            colors: [
              Color(0xFFF7E0E9),
              Color(0xFFF4F0EA),
              Color(0xFFF4F0EA),
            ],
          ),
        ),
      ),
    );
  }
}

class _CardPatternPainter extends CustomPainter {
  @override
  void paint(Canvas canvas, Size size) {
    final pink = Paint()
      ..color = const Color(0xFFEBC8BE).withValues(alpha: 0.72);
    final green = Paint()
      ..color = const Color(0xFFE8E2CE).withValues(alpha: 0.72);
    final stroke = Paint()
      ..color = const Color(0xFFE4D3BF).withValues(alpha: 0.72)
      ..strokeWidth = 9
      ..strokeCap = StrokeCap.round;

    canvas.drawCircle(Offset(size.width * 0.86, size.height * 0.18), 23, pink);
    canvas.drawCircle(Offset(size.width * 0.15, size.height * 0.78), 37, green);
    canvas.drawCircle(Offset(size.width * 0.87, size.height * 0.78), 27, pink);

    for (var i = 0; i < 4; i++) {
      canvas.drawLine(
        Offset(46 + i * 20, size.height * 0.34),
        Offset(82 + i * 20, size.height * 0.24),
        stroke,
      );
    }
  }

  @override
  bool shouldRepaint(covariant CustomPainter oldDelegate) => false;
}

class _SlideData {
  const _SlideData({
    required this.image,
    required this.label,
    required this.title,
    required this.body,
    required this.icon,
  });

  final String image;
  final String label;
  final String title;
  final String body;
  final IconData icon;
}
