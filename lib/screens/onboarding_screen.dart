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

  final PageController _controller = PageController();
  double _page = 0;
  int _index = 0;
  bool _finishing = false;

  static const List<_SlideData> _slides = [
    _SlideData(
      title: 'Lisa tooted korvi',
      body: 'Sirvi tooteid või otsi retsepte. Lisa kõik mida vajad ühte korvi.',
      tag: 'Päris',
      lottieUrl: 'https://assets3.lottiefiles.com/packages/lf20_UJNc2t.json',
      icon: Icons.add_rounded,
      accent: _pink,
      type: _IllustrationType.cart,
    ),
    _SlideData(
      title: 'Võrdleme kõigi poodidega',
      body:
          'Seivy kontrollib automaatselt Rimi, Selveri, Prisma, Coopi ja Maxima hinnad sinu ümbruses.',
      tag: 'Võrdlus',
      lottieUrl: 'https://assets4.lottiefiles.com/packages/lf20_gb5bmwlm.json',
      icon: Icons.search_rounded,
      accent: _pink,
      type: _IllustrationType.compare,
    ),
    _SlideData(
      title: 'Leia odavaim ostukorv',
      body:
          'Näed koheselt kust tuleb kogu korv kõige soodsamalt - mitte ühe toote hind, vaid kogusumma.',
      tag: 'Sääst',
      lottieUrl: 'https://assets3.lottiefiles.com/packages/lf20_RItkEz.json',
      icon: Icons.euro_rounded,
      accent: _pink,
      type: _IllustrationType.savings,
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
      duration: const Duration(milliseconds: 620),
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
        child: LayoutBuilder(
          builder: (context, constraints) {
            final height = constraints.maxHeight;
            final heroHeight = height * 0.56;

            return Stack(
              children: [
                const _SoftBackground(),
                Positioned(
                  top: 14,
                  left: 16,
                  child: _BrandPill(pink: _pink, dark: _dark),
                ),
                Positioned(
                  top: 14,
                  right: 16,
                  child: _SkipPill(
                    dark: _dark,
                    enabled: !_finishing,
                    onTap: _finish,
                  ),
                ),
                Positioned(
                  top: 64,
                  left: 0,
                  right: 0,
                  height: heroHeight - 58,
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
                      return _SlideHero(
                        slide: _slides[index],
                        page: _page,
                        index: index,
                      );
                    },
                  ),
                ),
                Positioned(
                  left: 0,
                  right: 0,
                  bottom: 0,
                  height: height * 0.45,
                  child: _BottomPanel(
                    slide: _slides[_index],
                    page: _page,
                    currentIndex: _index,
                    isLast: _isLast,
                    finishing: _finishing,
                    onPrimaryTap: _next,
                    dark: _dark,
                    pink: _pink,
                  ),
                ),
              ],
            );
          },
        ),
      ),
    );
  }
}

class _SlideHero extends StatelessWidget {
  const _SlideHero({
    required this.slide,
    required this.page,
    required this.index,
  });

  final _SlideData slide;
  final double page;
  final int index;

  @override
  Widget build(BuildContext context) {
    final distance = (page - index).clamp(-1.0, 1.0).toDouble();
    final hidden = distance.abs();
    final scale = ui.lerpDouble(1, 0.88, hidden)!;
    final rotate = ui.lerpDouble(0, distance * -0.045, hidden)!;

    return Transform.translate(
      offset: Offset(distance * -34, ui.lerpDouble(0, 18, hidden)!),
      child: Transform.rotate(
        angle: rotate,
        child: Transform.scale(
          scale: scale,
          child: Opacity(
            opacity: ui.lerpDouble(1, 0.58, hidden)!,
            child: Padding(
              padding: const EdgeInsets.symmetric(horizontal: 16),
              child: _IllustrationCard(slide: slide),
            ),
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
        color: const Color(0xFFF8EDE3),
        borderRadius: BorderRadius.circular(28),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withValues(alpha: 0.08),
            blurRadius: 26,
            offset: const Offset(0, 16),
          ),
        ],
      ),
      child: Stack(
        children: [
          Positioned.fill(child: CustomPaint(painter: _CardPatternPainter())),
          Positioned(
            top: 18,
            left: 18,
            child: _MiniTag(slide: slide),
          ),
          Positioned.fill(
            child: Padding(
              padding: const EdgeInsets.fromLTRB(20, 46, 20, 42),
              child: _MockupIllustration(type: slide.type),
            ),
          ),
          Positioned.fill(
            child: IgnorePointer(
              child: Opacity(
                opacity: 0.001,
                child: Lottie.network(
                  slide.lottieUrl,
                  fit: BoxFit.contain,
                  repeat: true,
                  animate: true,
                  frameRate: FrameRate.max,
                  errorBuilder: (context, error, stackTrace) {
                    return Icon(slide.icon, color: slide.accent);
                  },
                ),
              ),
            ),
          ),
          Positioned(
            right: 18,
            bottom: 18,
            child: _FloatingAccent(slide: slide),
          ),
        ],
      ),
    );
  }
}

class _MockupIllustration extends StatelessWidget {
  const _MockupIllustration({required this.type});

  final _IllustrationType type;

  @override
  Widget build(BuildContext context) {
    Widget child;
    if (type == _IllustrationType.cart) {
      child = const _CartIllustration();
    } else if (type == _IllustrationType.compare) {
      child = const _CompareIllustration();
    } else {
      child = const _SavingsIllustration();
    }

    return AnimatedSwitcher(
      duration: const Duration(milliseconds: 280),
      child: child,
    );
  }
}

class _CartIllustration extends StatelessWidget {
  const _CartIllustration();

  @override
  Widget build(BuildContext context) {
    return Stack(
      alignment: Alignment.center,
      children: [
        Positioned(
          top: 16,
          right: 60,
          child: _CircleButton(
            color: Color(0xFFC94B7C),
            icon: Icons.add_rounded,
            size: 50,
          ),
        ),
        Transform.rotate(
          angle: -0.07,
          child: Container(
            width: 205,
            height: 185,
            decoration: BoxDecoration(
              color: Colors.white.withValues(alpha: 0.28),
              borderRadius: BorderRadius.circular(34),
            ),
            child: Stack(
              alignment: Alignment.center,
              children: [
                Positioned(
                  top: 36,
                  child: Row(
                    children: const [
                      _ProductBlob(color: Color(0xFFF7C766), size: 42),
                      SizedBox(width: 8),
                      _ProductBlob(color: Color(0xFF78AE57), size: 58),
                      SizedBox(width: 8),
                      _ProductBlob(color: Color(0xFFE58A5D), size: 48),
                    ],
                  ),
                ),
                Positioned(
                  bottom: 32,
                  child: Icon(
                    Icons.shopping_cart_rounded,
                    size: 130,
                    color: Colors.black.withValues(alpha: 0.52),
                  ),
                ),
                Positioned(
                  bottom: 19,
                  left: 45,
                  child: _Wheel(),
                ),
                Positioned(
                  bottom: 19,
                  right: 45,
                  child: _Wheel(),
                ),
              ],
            ),
          ),
        ),
      ],
    );
  }
}

class _CompareIllustration extends StatelessWidget {
  const _CompareIllustration();

  static const List<_StoreRow> _rows = [
    _StoreRow('RIMI', Color(0xFFE21B2D), '78,56 €', 0.78),
    _StoreRow('SELVER', Color(0xFFFFC928), '72,43 €', 0.62),
    _StoreRow('PRISMA', Color(0xFF49A35B), '69,18 €', 0.86),
    _StoreRow('COOP', Color(0xFF0A75BC), '73,00 €', 0.54),
    _StoreRow('MAXIMA', Color(0xFF0A3F92), '79,21 €', 0.68),
  ];

  @override
  Widget build(BuildContext context) {
    return Stack(
      alignment: Alignment.center,
      children: [
        Positioned(
          top: 8,
          right: 58,
          child: _CircleButton(
            color: Color(0xFFC94B7C),
            icon: Icons.search_rounded,
            size: 48,
          ),
        ),
        Container(
          width: 220,
          padding: const EdgeInsets.all(18),
          decoration: BoxDecoration(
            color: Colors.white,
            borderRadius: BorderRadius.circular(26),
            boxShadow: [
              BoxShadow(
                color: Colors.black.withValues(alpha: 0.06),
                blurRadius: 18,
                offset: const Offset(0, 10),
              ),
            ],
          ),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              for (final row in _rows) ...[
                Row(
                  children: [
                    Container(
                      width: 52,
                      padding: const EdgeInsets.symmetric(vertical: 6),
                      decoration: BoxDecoration(
                        color: row.color,
                        borderRadius: BorderRadius.circular(6),
                      ),
                      child: Text(
                        row.name,
                        textAlign: TextAlign.center,
                        style: TextStyle(
                          color: row.color == const Color(0xFFFFC928)
                              ? const Color(0xFF1A1A1A)
                              : Colors.white,
                          fontSize: 10,
                          fontWeight: FontWeight.w900,
                        ),
                      ),
                    ),
                    const SizedBox(width: 10),
                    SizedBox(
                      width: 50,
                      child: Text(
                        row.price,
                        style: const TextStyle(
                          color: Color(0xFF1A1A1A),
                          fontSize: 11,
                          fontWeight: FontWeight.w800,
                        ),
                      ),
                    ),
                    const SizedBox(width: 8),
                    Expanded(
                      child: ClipRRect(
                        borderRadius: BorderRadius.circular(999),
                        child: LinearProgressIndicator(
                          value: row.progress,
                          minHeight: 8,
                          backgroundColor: const Color(0xFFEFE7DF),
                          valueColor: AlwaysStoppedAnimation<Color>(row.color),
                        ),
                      ),
                    ),
                  ],
                ),
                if (row != _rows.last) const SizedBox(height: 9),
              ],
            ],
          ),
        ),
        Positioned(
          bottom: 18,
          child: Container(
            padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
            decoration: BoxDecoration(
              color: Colors.white,
              borderRadius: BorderRadius.circular(18),
              boxShadow: [
                BoxShadow(
                  color: Colors.black.withValues(alpha: 0.05),
                  blurRadius: 12,
                  offset: const Offset(0, 7),
                ),
              ],
            ),
            child: const Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                Icon(
                  Icons.favorite_rounded,
                  color: Color(0xFFC94B7C),
                  size: 15,
                ),
                SizedBox(width: 6),
                Text(
                  'Hinnad uuenevad automaatselt',
                  style: TextStyle(
                    color: Color(0xFF8B7C72),
                    fontSize: 10,
                    fontWeight: FontWeight.w800,
                  ),
                ),
              ],
            ),
          ),
        ),
      ],
    );
  }
}

class _SavingsIllustration extends StatelessWidget {
  const _SavingsIllustration();

  @override
  Widget build(BuildContext context) {
    return Stack(
      alignment: Alignment.center,
      children: [
        Positioned(
          top: 10,
          left: 54,
          child: Transform.rotate(
            angle: -0.28,
            child: _CircleButton(
              color: Color(0xFFC94B7C),
              icon: Icons.percent_rounded,
              size: 45,
            ),
          ),
        ),
        Positioned(
          top: 20,
          right: 54,
          child: _Coin(size: 50),
        ),
        Positioned(
          top: 82,
          right: 24,
          child: _Coin(size: 42),
        ),
        Positioned(
          bottom: 38,
          child: Container(
            width: 190,
            height: 128,
            decoration: BoxDecoration(
              color: const Color(0xFFC94B7C),
              borderRadius: BorderRadius.circular(30),
              boxShadow: [
                BoxShadow(
                  color: const Color(0xFFC94B7C).withValues(alpha: 0.24),
                  blurRadius: 18,
                  offset: const Offset(0, 12),
                ),
              ],
            ),
            child: Stack(
              clipBehavior: Clip.none,
              children: [
                Positioned(
                  top: -58,
                  left: 28,
                  child: Row(
                    crossAxisAlignment: CrossAxisAlignment.end,
                    children: const [
                      _ProductBlob(color: Color(0xFF70A747), size: 54),
                      SizedBox(width: 8),
                      _ProductBlob(color: Color(0xFFF0B344), size: 84),
                      SizedBox(width: 8),
                      _ProductBlob(color: Color(0xFFD6E8F0), size: 66),
                    ],
                  ),
                ),
                Positioned.fill(
                  child: Icon(
                    Icons.shopping_basket_rounded,
                    color: Colors.white.withValues(alpha: 0.92),
                    size: 142,
                  ),
                ),
              ],
            ),
          ),
        ),
        Positioned(
          right: 22,
          bottom: 22,
          child: Transform.rotate(
            angle: 0.12,
            child: Container(
              width: 116,
              padding: const EdgeInsets.all(12),
              decoration: BoxDecoration(
                color: Colors.white,
                borderRadius: BorderRadius.circular(16),
                boxShadow: [
                  BoxShadow(
                    color: Colors.black.withValues(alpha: 0.08),
                    blurRadius: 15,
                    offset: const Offset(0, 8),
                  ),
                ],
              ),
              child: const Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Odavaim ostukorv',
                    style: TextStyle(
                      color: Color(0xFF8B7C72),
                      fontSize: 9,
                      fontWeight: FontWeight.w800,
                    ),
                  ),
                  SizedBox(height: 3),
                  Text(
                    '69,18 €',
                    style: TextStyle(
                      color: Color(0xFFC94B7C),
                      fontSize: 22,
                      fontWeight: FontWeight.w900,
                      letterSpacing: -0.7,
                    ),
                  ),
                  SizedBox(height: 5),
                  Row(
                    children: [
                      Icon(
                        Icons.storefront_rounded,
                        color: Color(0xFF49A35B),
                        size: 12,
                      ),
                      SizedBox(width: 4),
                      Text(
                        'Prisma',
                        style: TextStyle(
                          color: Color(0xFF49A35B),
                          fontSize: 10,
                          fontWeight: FontWeight.w900,
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ),
        ),
      ],
    );
  }
}

class _BottomPanel extends StatelessWidget {
  const _BottomPanel({
    required this.slide,
    required this.page,
    required this.currentIndex,
    required this.isLast,
    required this.finishing,
    required this.onPrimaryTap,
    required this.dark,
    required this.pink,
  });

  final _SlideData slide;
  final double page;
  final int currentIndex;
  final bool isLast;
  final bool finishing;
  final VoidCallback onPrimaryTap;
  final Color dark;
  final Color pink;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.fromLTRB(24, 23, 24, 22),
      decoration: const BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.vertical(top: Radius.circular(30)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          _PathIndicator(page: page, currentIndex: currentIndex),
          const SizedBox(height: 24),
          AnimatedSwitcher(
            duration: const Duration(milliseconds: 300),
            switchInCurve: Curves.easeOutCubic,
            transitionBuilder: (child, animation) {
              return FadeTransition(
                opacity: animation,
                child: SlideTransition(
                  position: Tween<Offset>(
                    begin: const Offset(0.05, 0),
                    end: Offset.zero,
                  ).animate(animation),
                  child: child,
                ),
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
                    fontSize: 27,
                    height: 1.02,
                    fontWeight: FontWeight.w900,
                    letterSpacing: -1,
                  ),
                ),
                const SizedBox(height: 14),
                Text(
                  slide.body,
                  style: TextStyle(
                    color: dark.withValues(alpha: 0.68),
                    fontSize: 15.5,
                    height: 1.48,
                    fontWeight: FontWeight.w700,
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
              onPressed: finishing ? null : onPrimaryTap,
              style: FilledButton.styleFrom(
                backgroundColor: isLast ? pink : Colors.black,
                disabledBackgroundColor: pink.withValues(alpha: 0.45),
                foregroundColor: Colors.white,
                elevation: 0,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(18),
                ),
              ),
              child: AnimatedSwitcher(
                duration: const Duration(milliseconds: 180),
                child: finishing
                    ? const SizedBox(
                        width: 22,
                        height: 22,
                        child: CircularProgressIndicator(
                          color: Colors.white,
                          strokeWidth: 2.5,
                        ),
                      )
                    : Row(
                        key: ValueKey(isLast),
                        mainAxisAlignment: MainAxisAlignment.center,
                        mainAxisSize: MainAxisSize.min,
                        children: [
                          Text(
                            isLast ? 'Alusta' : 'Järgmine',
                            style: const TextStyle(
                              fontSize: 16,
                              fontWeight: FontWeight.w900,
                            ),
                          ),
                          const SizedBox(width: 10),
                          Icon(
                            isLast
                                ? Icons.shopping_basket_rounded
                                : Icons.arrow_forward_rounded,
                            size: 21,
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

class _PathIndicator extends StatelessWidget {
  const _PathIndicator({
    required this.page,
    required this.currentIndex,
  });

  final double page;
  final int currentIndex;

  @override
  Widget build(BuildContext context) {
    final progress = (page / 2).clamp(0.0, 1.0).toDouble();

    return SizedBox(
      height: 42,
      child: CustomPaint(
        painter: _PathIndicatorPainter(progress: progress),
        child: Row(
          children: List.generate(3, (index) {
            final active = index <= currentIndex;
            final selected = index == currentIndex;

            return Expanded(
              child: Center(
                child: AnimatedContainer(
                  duration: const Duration(milliseconds: 260),
                  curve: Curves.easeOutCubic,
                  width: selected ? 106 : 42,
                  height: 34,
                  decoration: BoxDecoration(
                    color: active
                        ? const Color(0xFFC94B7C)
                        : const Color(0xFFF3EEE8),
                    borderRadius: BorderRadius.circular(999),
                    boxShadow: selected
                        ? [
                            BoxShadow(
                              color: const Color(0xFFC94B7C).withValues(alpha: 0.24),
                              blurRadius: 14,
                              offset: const Offset(0, 7),
                            ),
                          ]
                        : null,
                  ),
                  child: Row(
                    mainAxisAlignment: MainAxisAlignment.center,
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      Icon(
                        active ? Icons.check_rounded : null,
                        color: Colors.white,
                        size: active ? 17 : 0,
                      ),
                      if (!active)
                        Text(
                          '${index + 1}',
                          style: const TextStyle(
                            color: Color(0xFFBEB4AC),
                            fontWeight: FontWeight.w900,
                          ),
                        ),
                      if (selected) ...[
                        const SizedBox(width: 7),
                        Text(
                          '${index + 1}',
                          style: const TextStyle(
                            color: Colors.white,
                            fontWeight: FontWeight.w900,
                          ),
                        ),
                      ],
                    ],
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
      height: 46,
      padding: const EdgeInsets.symmetric(horizontal: 12),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(17),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withValues(alpha: 0.04),
            blurRadius: 18,
            offset: const Offset(0, 8),
          ),
        ],
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Container(
            width: 28,
            height: 28,
            decoration: BoxDecoration(
              color: pink,
              borderRadius: BorderRadius.circular(9),
            ),
            child: const Icon(
              Icons.bar_chart_rounded,
              color: Colors.white,
              size: 18,
            ),
          ),
          const SizedBox(width: 8),
          Text(
            'Seivy',
            style: TextStyle(
              color: dark,
              fontSize: 16,
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
    required this.dark,
    required this.enabled,
    required this.onTap,
  });

  final Color dark;
  final bool enabled;
  final VoidCallback onTap;

  @override
  Widget build(BuildContext context) {
    return Material(
      color: Colors.white,
      borderRadius: BorderRadius.circular(18),
      child: InkWell(
        onTap: enabled ? onTap : null,
        borderRadius: BorderRadius.circular(18),
        child: Padding(
          padding: const EdgeInsets.symmetric(horizontal: 17, vertical: 13),
          child: Text(
            'Jäta vahele',
            style: TextStyle(
              color: dark.withValues(alpha: 0.64),
              fontSize: 14,
              fontWeight: FontWeight.w900,
            ),
          ),
        ),
      ),
    );
  }
}

class _MiniTag extends StatelessWidget {
  const _MiniTag({required this.slide});

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
          Icon(slide.icon, color: slide.accent, size: 16),
          const SizedBox(width: 6),
          Text(
            slide.tag,
            style: TextStyle(
              color: slide.type == _IllustrationType.savings
                  ? const Color(0xFF4FA34D)
                  : slide.accent,
              fontSize: 12,
              fontWeight: FontWeight.w900,
            ),
          ),
        ],
      ),
    );
  }
}

class _FloatingAccent extends StatelessWidget {
  const _FloatingAccent({required this.slide});

  final _SlideData slide;

  @override
  Widget build(BuildContext context) {
    if (slide.type == _IllustrationType.cart) {
      return _CircleButton(color: slide.accent, icon: Icons.add_rounded, size: 44);
    }

    if (slide.type == _IllustrationType.compare) {
      return Container(
        padding: const EdgeInsets.symmetric(horizontal: 11, vertical: 9),
        decoration: BoxDecoration(
          color: Colors.white,
          borderRadius: BorderRadius.circular(15),
        ),
        child: const Icon(
          Icons.verified_rounded,
          color: Color(0xFF4FA34D),
          size: 20,
        ),
      );
    }

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 13, vertical: 10),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(18),
      ),
      child: const Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(Icons.auto_awesome_rounded, color: Color(0xFF4FA34D), size: 17),
          SizedBox(width: 6),
          Text(
            '-18%',
            style: TextStyle(
              color: Color(0xFF4FA34D),
              fontWeight: FontWeight.w900,
              fontSize: 15,
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
    return Stack(
      children: [
        Positioned.fill(
          child: DecoratedBox(
            decoration: BoxDecoration(
              gradient: LinearGradient(
                begin: Alignment.topCenter,
                end: Alignment.bottomCenter,
                colors: [
                  const Color(0xFFC94B7C).withValues(alpha: 0.09),
                  const Color(0xFFF4F0EA),
                  const Color(0xFFF4F0EA),
                ],
              ),
            ),
          ),
        ),
        const Positioned(
          top: -70,
          left: -80,
          child: _BlurBlob(
            color: Color(0x22C94B7C),
            size: 240,
          ),
        ),
        const Positioned(
          top: 90,
          right: -70,
          child: _BlurBlob(
            color: Color(0x2268B763),
            size: 220,
          ),
        ),
      ],
    );
  }
}

class _BlurBlob extends StatelessWidget {
  const _BlurBlob({
    required this.color,
    required this.size,
  });

  final Color color;
  final double size;

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

class _CircleButton extends StatelessWidget {
  const _CircleButton({
    required this.color,
    required this.icon,
    required this.size,
  });

  final Color color;
  final IconData icon;
  final double size;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: size,
      height: size,
      decoration: BoxDecoration(
        color: color,
        shape: BoxShape.circle,
        boxShadow: [
          BoxShadow(
            color: color.withValues(alpha: 0.24),
            blurRadius: 14,
            offset: const Offset(0, 8),
          ),
        ],
      ),
      child: Icon(icon, color: Colors.white, size: size * 0.5),
    );
  }
}

class _ProductBlob extends StatelessWidget {
  const _ProductBlob({
    required this.color,
    required this.size,
  });

  final Color color;
  final double size;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: size,
      height: size,
      decoration: BoxDecoration(
        color: color,
        borderRadius: BorderRadius.circular(size * 0.38),
        boxShadow: [
          BoxShadow(
            color: color.withValues(alpha: 0.24),
            blurRadius: 12,
            offset: const Offset(0, 7),
          ),
        ],
      ),
    );
  }
}

class _Wheel extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Container(
      width: 17,
      height: 17,
      decoration: BoxDecoration(
        color: Colors.white,
        shape: BoxShape.circle,
        border: Border.all(color: Colors.black.withValues(alpha: 0.52), width: 3),
      ),
    );
  }
}

class _Coin extends StatelessWidget {
  const _Coin({required this.size});

  final double size;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: size,
      height: size,
      decoration: BoxDecoration(
        color: const Color(0xFFF4B342),
        shape: BoxShape.circle,
        border: Border.all(color: Colors.white, width: 4),
        boxShadow: [
          BoxShadow(
            color: const Color(0xFFF4B342).withValues(alpha: 0.24),
            blurRadius: 14,
            offset: const Offset(0, 8),
          ),
        ],
      ),
      child: const Center(
        child: Text(
          '€',
          style: TextStyle(
            color: Colors.white,
            fontSize: 23,
            fontWeight: FontWeight.w900,
          ),
        ),
      ),
    );
  }
}

class _PathIndicatorPainter extends CustomPainter {
  const _PathIndicatorPainter({required this.progress});

  final double progress;

  @override
  void paint(Canvas canvas, Size size) {
    final y = size.height / 2;
    final start = Offset(size.width / 6, y);
    final middle = Offset(size.width / 2, y);
    final end = Offset(size.width * 5 / 6, y);

    final trackPaint = Paint()
      ..color = const Color(0xFFF3EEE8)
      ..strokeWidth = 8
      ..strokeCap = StrokeCap.round
      ..style = PaintingStyle.stroke;

    final activePaint = Paint()
      ..shader = const LinearGradient(
        colors: [Color(0xFFC94B7C), Color(0xFFC94B7C)],
      ).createShader(Rect.fromLTWH(0, 0, size.width, size.height))
      ..strokeWidth = 8
      ..strokeCap = StrokeCap.round
      ..style = PaintingStyle.stroke;

    final path = Path()
      ..moveTo(start.dx, start.dy)
      ..cubicTo(
        size.width * 0.31,
        y - 8,
        size.width * 0.36,
        y + 8,
        middle.dx,
        middle.dy,
      )
      ..cubicTo(
        size.width * 0.64,
        y - 8,
        size.width * 0.69,
        y + 8,
        end.dx,
        end.dy,
      );

    canvas.drawPath(path, trackPaint);

    final metric = path.computeMetrics().first;
    final activePath = metric.extractPath(0, metric.length * progress);
    canvas.drawPath(activePath, activePaint);
  }

  @override
  bool shouldRepaint(covariant _PathIndicatorPainter oldDelegate) {
    return oldDelegate.progress != progress;
  }
}

class _CardPatternPainter extends CustomPainter {
  @override
  void paint(Canvas canvas, Size size) {
    final palePink = Paint()..color = const Color(0xFFC94B7C).withValues(alpha: 0.08);
    final paleGreen = Paint()..color = const Color(0xFF78AE57).withValues(alpha: 0.07);

    canvas.drawCircle(Offset(size.width * 0.86, size.height * 0.16), 24, palePink);
    canvas.drawCircle(Offset(size.width * 0.13, size.height * 0.73), 38, paleGreen);
    canvas.drawCircle(Offset(size.width * 0.88, size.height * 0.72), 28, palePink);

    final brush = Paint()
      ..color = const Color(0xFFB59A89).withValues(alpha: 0.13)
      ..strokeWidth = 9
      ..strokeCap = StrokeCap.round;

    for (var i = 0; i < 4; i++) {
      final x = 34.0 + (i * 17);
      canvas.drawLine(
        Offset(x, size.height * 0.34),
        Offset(x + 28, size.height * 0.26),
        brush,
      );
    }
  }

  @override
  bool shouldRepaint(covariant CustomPainter oldDelegate) => false;
}

enum _IllustrationType { cart, compare, savings }

class _SlideData {
  const _SlideData({
    required this.title,
    required this.body,
    required this.tag,
    required this.lottieUrl,
    required this.icon,
    required this.accent,
    required this.type,
  });

  final String title;
  final String body;
  final String tag;
  final String lottieUrl;
  final IconData icon;
  final Color accent;
  final _IllustrationType type;
}

class _StoreRow {
  const _StoreRow(this.name, this.color, this.price, this.progress);

  final String name;
  final Color color;
  final String price;
  final double progress;
}
