import 'package:flutter/material.dart';
import 'package:grocery_app/providers/basket_provider.dart';
import 'package:grocery_app/screens/categories_main_screen.dart';
import 'package:grocery_app/services/auth_service.dart';
import 'package:grocery_app/services/session.dart';
import 'package:provider/provider.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  String? userEmail;
  String? firstName;

  @override
  void initState() {
    super.initState();
    _loadProfile();
  }

  Future<void> _loadProfile() async {
    final profile = await AuthService().getProfile();
    if (!mounted) return;
    setState(() {
      userEmail = profile?['email'] ?? 'Kasutaja';
      firstName = profile?['first_name'] ?? '';
    });
  }

  Future<void> _logout() async {
    await AuthService().logout();
    if (mounted) Navigator.pushReplacementNamed(context, '/login');
  }

  void _navigateToCompare() => Navigator.pushNamed(context, '/compare');
  void _navigateToBasket() => Navigator.pushNamed(context, '/basket');
  void _navigateToProducts() => Navigator.push(
        context,
        MaterialPageRoute(builder: (_) => const CategoriesMainScreen()),
      );

  Future<void> _navigateToBasketHistory() async {
    final token = await Session.getToken();
    if (!mounted) return;
    if (token == null || token.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Palun logi sisse.')),
      );
      Navigator.pushNamed(context, '/login');
    } else {
      Navigator.pushNamed(context, '/basket-history');
    }
  }

  @override
  Widget build(BuildContext context) {
    final basketProvider = Provider.of<BasketProvider>(context);
    final itemCount = basketProvider.totalQuantity;
    final name = (firstName != null && firstName!.isNotEmpty)
        ? firstName!
        : (userEmail ?? '');

    return Scaffold(
      backgroundColor: const Color(0xFFF0EDE8),
      appBar: AppBar(
        backgroundColor: const Color(0xFFF0EDE8),
        elevation: 0,
        title: const Text(
          'Hinnavõrdlus',
          style: TextStyle(
            color: Color(0xFF1A1A1A),
            fontWeight: FontWeight.w800,
            fontSize: 22,
          ),
        ),
        actions: [
          IconButton(
            icon: const Icon(Icons.logout_rounded, color: Color(0xFF1A1A1A)),
            onPressed: _logout,
          ),
        ],
      ),
      body: SafeArea(
        child: Padding(
          padding: const EdgeInsets.fromLTRB(16, 8, 16, 16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                'Tere tulemast,',
                style: TextStyle(fontSize: 15, color: Colors.grey.shade600),
              ),
              Text(
                name,
                style: const TextStyle(
                  fontSize: 28,
                  fontWeight: FontWeight.w800,
                  color: Color(0xFF1A1A1A),
                  letterSpacing: -0.5,
                ),
              ),
              const SizedBox(height: 20),
              Expanded(
                child: LayoutBuilder(
                  builder: (context, constraints) {
                    return PuzzleGrid(
                      width: constraints.maxWidth,
                      height: constraints.maxHeight,
                      itemCount: itemCount,
                      onCompare: _navigateToCompare,
                      onProducts: _navigateToProducts,
                      onBasket: _navigateToBasket,
                      onHistory: _navigateToBasketHistory,
                    );
                  },
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class PuzzleGrid extends StatelessWidget {
  const PuzzleGrid({
    super.key,
    required this.width,
    required this.height,
    required this.itemCount,
    required this.onCompare,
    required this.onProducts,
    required this.onBasket,
    required this.onHistory,
  });

  final double width;
  final double height;
  final int itemCount;
  final VoidCallback onCompare;
  final VoidCallback onProducts;
  final VoidCallback onBasket;
  final VoidCallback onHistory;

  @override
  Widget build(BuildContext context) {
    const pieceWidth = SvgPuzzlePieceClipper.sourceWidth;
    const pieceHeight = SvgPuzzlePieceClipper.sourceHeight;
    const stepX = SvgPuzzlePieceClipper.connectionStepX;
    const stepY = SvgPuzzlePieceClipper.connectionStepY;

    const groupSourceWidth = pieceWidth + stepX;
    const groupSourceHeight = pieceHeight + stepY;
    const fifthScaleFactor = 0.66;
    const gapSource = 26.0;
    const fullSourceHeight =
        groupSourceHeight + gapSource + (pieceHeight * fifthScaleFactor);

    final widthScale = (width * 0.98) / groupSourceWidth;
    final heightScale = (height * 0.96) / fullSourceHeight;
    final scale = widthScale < heightScale ? widthScale : heightScale;

    final pieceW = pieceWidth * scale;
    final pieceH = pieceHeight * scale;
    final stepW = stepX * scale;
    final stepH = stepY * scale;
    final groupWidth = groupSourceWidth * scale;
    final groupHeight = groupSourceHeight * scale;
    final fifthW = pieceW * fifthScaleFactor;
    final fifthH = pieceH * fifthScaleFactor;
    final fifthGap = gapSource * scale;

    final groupLeft = ((width - groupWidth) / 2).clamp(0.0, width).toDouble();
    final groupTop = ((height - (groupHeight + fifthGap + fifthH)) / 2)
        .clamp(0.0, height - groupHeight)
        .toDouble();
    final fifthLeft = (groupLeft + ((groupWidth - fifthW) / 2))
        .clamp(0.0, width - fifthW)
        .toDouble();
    final fifthTop = (groupTop + groupHeight + fifthGap)
        .clamp(0.0, height - fifthH)
        .toDouble();

    return SizedBox(
      width: width,
      height: height,
      child: Stack(
        clipBehavior: Clip.none,
        children: [
          Positioned(
            left: groupLeft + stepW,
            top: groupTop,
            child: PuzzlePieceButton(
              icon: Icons.shopping_cart_rounded,
              label: 'Sirvi\ntooteid',
              color: const Color(0xFF1476C9),
              width: pieceW,
              height: pieceH,
              onPressed: onProducts,
            ),
          ),
          Positioned(
            left: groupLeft + stepW,
            top: groupTop + stepH,
            child: PuzzlePieceButton(
              icon: Icons.history_rounded,
              label: 'Korvi\najalugu',
              color: const Color(0xFF55C600),
              width: pieceW,
              height: pieceH,
              onPressed: onHistory,
            ),
          ),
          Positioned(
            left: groupLeft,
            top: groupTop,
            child: PuzzlePieceButton(
              icon: Icons.insert_chart_rounded,
              label: 'Võrdle\nkorvi',
              color: const Color(0xFFE91E63),
              width: pieceW,
              height: pieceH,
              onPressed: onCompare,
            ),
          ),
          Positioned(
            left: groupLeft,
            top: groupTop + stepH,
            child: PuzzlePieceButton(
              icon: Icons.shopping_basket_rounded,
              label: 'Ostukorv${itemCount > 0 ? "\n($itemCount)" : ""}',
              color: const Color(0xFFFFD600),
              width: pieceW,
              height: pieceH,
              onPressed: onBasket,
            ),
          ),
          Positioned(
            left: fifthLeft,
            top: fifthTop,
            child: Transform.rotate(
              angle: -0.06,
              child: PuzzlePieceButton(
                icon: Icons.restaurant_menu_rounded,
                label: 'Retseptid',
                color: Colors.white,
                foregroundColor: const Color(0xFF1A1A1A),
                borderColor: const Color(0x66808080),
                width: fifthW,
                height: fifthH,
                onPressed: () {
                  ScaffoldMessenger.of(context)
                    ..hideCurrentSnackBar()
                    ..showSnackBar(
                      const SnackBar(content: Text('Retseptid tulekul!')),
                    );
                },
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class PuzzlePieceButton extends StatelessWidget {
  const PuzzlePieceButton({
    super.key,
    required this.icon,
    required this.label,
    required this.color,
    required this.width,
    required this.height,
    required this.onPressed,
    this.foregroundColor = Colors.white,
    this.borderColor = const Color(0x55FFFFFF),
  });

  final IconData icon;
  final String label;
  final Color color;
  final Color foregroundColor;
  final Color borderColor;
  final double width;
  final double height;
  final VoidCallback onPressed;

  @override
  Widget build(BuildContext context) {
    const clipper = SvgPuzzlePieceClipper();
    final labelStyle = TextStyle(
      color: foregroundColor,
      fontWeight: FontWeight.w800,
      fontSize: (height * 0.145).clamp(16.0, 23.0).toDouble(),
      height: 1.12,
      shadows: [
        Shadow(
          color: _alphaColor(Colors.black, color == Colors.white ? 0.0 : 0.35),
          blurRadius: 3,
          offset: const Offset(0, 2),
        ),
      ],
    );

    return Semantics(
      button: true,
      label: label.replaceAll('\n', ' '),
      child: SizedBox(
        width: width,
        height: height,
        child: Stack(
          children: [
            PhysicalShape(
              clipper: clipper,
              clipBehavior: Clip.antiAlias,
              color: color,
              elevation: 7,
              shadowColor: _alphaColor(Colors.black, 0.25),
              child: DecoratedBox(
                decoration: BoxDecoration(
                  gradient: LinearGradient(
                    begin: Alignment.topLeft,
                    end: Alignment.bottomRight,
                    colors: [
                      _lightenColor(color, color == Colors.white ? 0.0 : 0.18),
                      color,
                      _darkenColor(color, color == Colors.white ? 0.04 : 0.10),
                    ],
                  ),
                ),
                child: Material(
                  color: Colors.transparent,
                  child: InkWell(
                    onTap: onPressed,
                    splashColor: _alphaColor(foregroundColor, 0.22),
                    highlightColor: _alphaColor(foregroundColor, 0.10),
                    child: Stack(
                      children: [
                        Positioned(
                          left: width * 0.20,
                          top: height * 0.30,
                          width: width * 0.52,
                          height: height * 0.43,
                          child: FittedBox(
                            fit: BoxFit.scaleDown,
                            child: Column(
                              mainAxisSize: MainAxisSize.min,
                              children: [
                                Icon(
                                  icon,
                                  color: foregroundColor,
                                  size: (height * 0.23)
                                      .clamp(28.0, 44.0)
                                      .toDouble(),
                                ),
                                SizedBox(height: height * 0.012),
                                Text(
                                  label,
                                  maxLines: 2,
                                  overflow: TextOverflow.visible,
                                  textAlign: TextAlign.center,
                                  style: labelStyle,
                                ),
                              ],
                            ),
                          ),
                        ),
                      ],
                    ),
                  ),
                ),
              ),
            ),
            Positioned.fill(
              child: IgnorePointer(
                child: CustomPaint(
                  painter: PuzzlePieceBorderPainter(
                    clipper: clipper,
                    color: borderColor,
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

class SvgPuzzlePieceClipper extends CustomClipper<Path> {
  const SvgPuzzlePieceClipper();

  static const double sourceWidth = 549;
  static const double sourceHeight = 358;
  static const double connectionStepX = 444;
  static const double connectionStepY = 253;

  @override
  Path getClip(Size size) {
    final scaleX = size.width / sourceWidth;
    final scaleY = size.height / sourceHeight;

    final path = Path()
      ..moveTo(0.0, 105.0)
      ..lineTo(165.0, 105.0)
      ..cubicTo(
        180.0,
        105.0,
        188.0,
        91.0,
        179.0,
        79.0,
      )
      ..cubicTo(
        168.0,
        63.0,
        169.0,
        43.0,
        185.0,
        27.0,
      )
      ..cubicTo(
        205.0,
        7.0,
        243.0,
        0.0,
        277.0,
        11.0,
      )
      ..cubicTo(
        299.0,
        18.0,
        306.0,
        33.0,
        294.0,
        52.0,
      )
      ..cubicTo(
        282.0,
        71.0,
        287.0,
        105.0,
        313.0,
        105.0,
      )
      ..lineTo(444.0, 105.0)
      ..lineTo(444.0, 171.0)
      ..cubicTo(
        444.0,
        187.0,
        460.0,
        195.0,
        473.0,
        185.0,
      )
      ..cubicTo(
        495.0,
        169.0,
        522.0,
        185.0,
        535.0,
        215.0,
      )
      ..cubicTo(
        549.0,
        249.0,
        537.0,
        288.0,
        513.0,
        297.0,
      )
      ..cubicTo(
        497.0,
        303.0,
        484.0,
        294.0,
        474.0,
        281.0,
      )
      ..cubicTo(
        464.0,
        268.0,
        444.0,
        274.0,
        444.0,
        291.0,
      )
      ..lineTo(444.0, 358.0)
      ..lineTo(313.0, 358.0)
      ..cubicTo(
        287.0,
        358.0,
        282.0,
        324.0,
        294.0,
        305.0,
      )
      ..cubicTo(
        306.0,
        286.0,
        299.0,
        271.0,
        277.0,
        264.0,
      )
      ..cubicTo(
        243.0,
        253.0,
        205.0,
        260.0,
        185.0,
        280.0,
      )
      ..cubicTo(
        169.0,
        296.0,
        168.0,
        316.0,
        179.0,
        332.0,
      )
      ..cubicTo(
        188.0,
        344.0,
        180.0,
        358.0,
        165.0,
        358.0,
      )
      ..lineTo(0.0, 358.0)
      ..lineTo(0.0, 291.0)
      ..cubicTo(
        0.0,
        274.0,
        20.0,
        268.0,
        30.0,
        281.0,
      )
      ..cubicTo(
        40.0,
        294.0,
        53.0,
        303.0,
        69.0,
        297.0,
      )
      ..cubicTo(
        93.0,
        288.0,
        105.0,
        249.0,
        91.0,
        215.0,
      )
      ..cubicTo(
        78.0,
        185.0,
        51.0,
        169.0,
        29.0,
        185.0,
      )
      ..cubicTo(
        16.0,
        195.0,
        0.0,
        187.0,
        0.0,
        171.0,
      )
      ..lineTo(0.0, 105.0)
      ..close();

    return path.transform(
      Matrix4.diagonal3Values(scaleX, scaleY, 1).storage,
    );
  }

  @override
  bool shouldReclip(SvgPuzzlePieceClipper oldClipper) => false;
}

class PuzzlePieceBorderPainter extends CustomPainter {
  const PuzzlePieceBorderPainter({
    required this.clipper,
    required this.color,
  });

  final SvgPuzzlePieceClipper clipper;
  final Color color;

  @override
  void paint(Canvas canvas, Size size) {
    final path = clipper.getClip(size);
    final paint = Paint()
      ..color = color
      ..style = PaintingStyle.stroke
      ..strokeWidth = 2.2;

    canvas.drawPath(path, paint);
  }

  @override
  bool shouldRepaint(PuzzlePieceBorderPainter oldDelegate) {
    return clipper != oldDelegate.clipper || color != oldDelegate.color;
  }
}

Color _alphaColor(Color color, double opacity) {
  return color.withAlpha((opacity.clamp(0, 1) * 255).round());
}

Color _lightenColor(Color color, double amount) {
  return Color.fromARGB(
    color.alpha,
    color.red + ((255 - color.red) * amount).round(),
    color.green + ((255 - color.green) * amount).round(),
    color.blue + ((255 - color.blue) * amount).round(),
  );
}

Color _darkenColor(Color color, double amount) {
  return Color.fromARGB(
    color.alpha,
    (color.red * (1 - amount)).round(),
    (color.green * (1 - amount)).round(),
    (color.blue * (1 - amount)).round(),
  );
}
