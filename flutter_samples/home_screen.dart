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
    const pieceSourceSize = 300.0;
    const connectorSourceSize = 42.0;
    const pieceGapSource = 6.0;
    const stepSource =
        pieceSourceSize - (connectorSourceSize * 2) + pieceGapSource;
    const groupSourceSize = pieceSourceSize + stepSource;
    const fifthScaleFactor = 0.66;
    const gapSource = 26.0;
    const fullSourceHeight =
        groupSourceSize + gapSource + (pieceSourceSize * fifthScaleFactor);

    final widthScale = (width * 0.98) / groupSourceSize;
    final heightScale = (height * 0.96) / fullSourceHeight;
    final scale = widthScale < heightScale ? widthScale : heightScale;

    final pieceSize = pieceSourceSize * scale;
    final connectorSize = connectorSourceSize * scale;
    final step = stepSource * scale;
    final groupSize = groupSourceSize * scale;
    final fifthSize = pieceSize * fifthScaleFactor;
    final fifthConnectorSize = connectorSize * fifthScaleFactor;
    final fifthGap = gapSource * scale;

    final groupLeft = ((width - groupSize) / 2).clamp(0.0, width).toDouble();
    final groupTop = ((height - (groupSize + fifthGap + fifthSize)) / 2)
        .clamp(0.0, height - groupSize)
        .toDouble();
    final fifthLeft = (groupLeft + ((groupSize - fifthSize) / 2))
        .clamp(0.0, width - fifthSize)
        .toDouble();
    final fifthTop = (groupTop + groupSize + fifthGap)
        .clamp(0.0, height - fifthSize)
        .toDouble();

    return SizedBox(
      width: width,
      height: height,
      child: Stack(
        clipBehavior: Clip.none,
        children: [
          Positioned(
            left: groupLeft + step,
            top: groupTop + step,
            child: PuzzlePieceButton(
              icon: Icons.history_rounded,
              label: 'Korvi\najalugu',
              color: const Color(0xFF55C600),
              edges: const PuzzlePieceEdges(
                top: PuzzleConnector.socket,
                right: PuzzleConnector.socket,
                bottom: PuzzleConnector.knob,
                left: PuzzleConnector.socket,
              ),
              size: pieceSize,
              connectorSize: connectorSize,
              onPressed: onHistory,
            ),
          ),
          Positioned(
            left: groupLeft + step,
            top: groupTop,
            child: PuzzlePieceButton(
              icon: Icons.shopping_cart_rounded,
              label: 'Sirvi\ntooteid',
              color: const Color(0xFF1476C9),
              edges: const PuzzlePieceEdges(
                top: PuzzleConnector.socket,
                right: PuzzleConnector.knob,
                bottom: PuzzleConnector.knob,
                left: PuzzleConnector.socket,
              ),
              size: pieceSize,
              connectorSize: connectorSize,
              onPressed: onProducts,
            ),
          ),
          Positioned(
            left: groupLeft,
            top: groupTop,
            child: PuzzlePieceButton(
              icon: Icons.insert_chart_rounded,
              label: 'Võrdle\nkorvi',
              color: const Color(0xFFE91E63),
              edges: const PuzzlePieceEdges(
                top: PuzzleConnector.knob,
                right: PuzzleConnector.knob,
                bottom: PuzzleConnector.socket,
                left: PuzzleConnector.socket,
              ),
              size: pieceSize,
              connectorSize: connectorSize,
              onPressed: onCompare,
            ),
          ),
          Positioned(
            left: groupLeft,
            top: groupTop + step,
            child: PuzzlePieceButton(
              icon: Icons.shopping_basket_rounded,
              label: 'Ostukorv${itemCount > 0 ? "\n($itemCount)" : ""}',
              color: const Color(0xFFFFD600),
              foregroundColor: const Color(0xFF1A1A1A),
              edges: const PuzzlePieceEdges(
                top: PuzzleConnector.knob,
                right: PuzzleConnector.knob,
                bottom: PuzzleConnector.socket,
                left: PuzzleConnector.knob,
              ),
              size: pieceSize,
              connectorSize: connectorSize,
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
                edges: const PuzzlePieceEdges(
                  top: PuzzleConnector.knob,
                  right: PuzzleConnector.knob,
                  bottom: PuzzleConnector.socket,
                  left: PuzzleConnector.socket,
                ),
                size: fifthSize,
                connectorSize: fifthConnectorSize,
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
    required this.edges,
    required this.size,
    required this.connectorSize,
    required this.onPressed,
    this.foregroundColor = Colors.white,
    this.borderColor = const Color(0xD01A1A1A),
  });

  final IconData icon;
  final String label;
  final Color color;
  final Color foregroundColor;
  final Color borderColor;
  final PuzzlePieceEdges edges;
  final double size;
  final double connectorSize;
  final VoidCallback onPressed;

  @override
  Widget build(BuildContext context) {
    final clipper = RoundedPuzzlePieceClipper(
      edges: edges,
      connectorSize: connectorSize,
    );
    final labelStyle = TextStyle(
      color: foregroundColor,
      fontWeight: FontWeight.w900,
      fontSize: (size * 0.068).clamp(16.0, 24.0).toDouble(),
      height: 1.04,
      letterSpacing: -0.45,
      shadows: [
        Shadow(
          color: _alphaColor(Colors.black, color == Colors.white ? 0.0 : 0.28),
          blurRadius: 3,
          offset: const Offset(0, 1.4),
        ),
      ],
    );

    return Semantics(
      button: true,
      label: label.replaceAll('\n', ' '),
      child: SizedBox.square(
        dimension: size,
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
                          left: size * 0.22,
                          top: size * 0.28,
                          width: size * 0.50,
                          height: size * 0.46,
                          child: FittedBox(
                            fit: BoxFit.scaleDown,
                            child: Column(
                              mainAxisSize: MainAxisSize.min,
                              children: [
                                Icon(
                                  icon,
                                  color: foregroundColor,
                                  size: (size * 0.16)
                                      .clamp(30.0, 46.0)
                                      .toDouble(),
                                ),
                                SizedBox(height: size * 0.012),
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

class PuzzlePieceEdges {
  const PuzzlePieceEdges({
    required this.top,
    required this.right,
    required this.bottom,
    required this.left,
  });

  final PuzzleConnector top;
  final PuzzleConnector right;
  final PuzzleConnector bottom;
  final PuzzleConnector left;

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        other is PuzzlePieceEdges &&
            top == other.top &&
            right == other.right &&
            bottom == other.bottom &&
            left == other.left;
  }

  @override
  int get hashCode => Object.hash(top, right, bottom, left);
}

enum PuzzleConnector {
  flat,
  knob,
  socket,
}

class RoundedPuzzlePieceClipper extends CustomClipper<Path> {
  const RoundedPuzzlePieceClipper({
    required this.edges,
    required this.connectorSize,
  });

  final PuzzlePieceEdges edges;
  final double connectorSize;

  @override
  Path getClip(Size size) {
    final rect = Rect.fromLTWH(
      connectorSize,
      connectorSize,
      size.width - (connectorSize * 2),
      size.height - (connectorSize * 2),
    );
    final depth = connectorSize * 0.95;

    final path = Path()..moveTo(rect.left, rect.top);

    _drawHorizontalEdge(
      path,
      start: Offset(rect.left, rect.top),
      end: Offset(rect.right, rect.top),
      connector: edges.top,
      depth: depth,
      outwardSign: -1,
    );
    _drawVerticalEdge(
      path,
      start: Offset(rect.right, rect.top),
      end: Offset(rect.right, rect.bottom),
      connector: edges.right,
      depth: depth,
      outwardSign: 1,
    );
    _drawHorizontalEdge(
      path,
      start: Offset(rect.right, rect.bottom),
      end: Offset(rect.left, rect.bottom),
      connector: edges.bottom,
      depth: depth,
      outwardSign: 1,
    );
    _drawVerticalEdge(
      path,
      start: Offset(rect.left, rect.bottom),
      end: Offset(rect.left, rect.top),
      connector: edges.left,
      depth: depth,
      outwardSign: -1,
    );

    return path..close();
  }

  void _drawHorizontalEdge(
    Path path, {
    required Offset start,
    required Offset end,
    required PuzzleConnector connector,
    required double depth,
    required double outwardSign,
  }) {
    if (connector == PuzzleConnector.flat) {
      path.lineTo(end.dx, end.dy);
      return;
    }

    final direction = end.dx > start.dx ? 1.0 : -1.0;
    final length = (end.dx - start.dx).abs();
    final connectorStart = start.dx + (direction * length * 0.32);
    final connectorEnd = start.dx + (direction * length * 0.68);
    final center = start.dx + (direction * length * 0.50);
    final side = connector == PuzzleConnector.knob ? outwardSign : -outwardSign;
    final y = start.dy;

    path
      ..lineTo(connectorStart, y)
      ..cubicTo(
        connectorStart + (direction * length * 0.05),
        y,
        center - (direction * length * 0.18),
        y + (side * depth * 0.06),
        center - (direction * length * 0.16),
        y + (side * depth * 0.46),
      )
      ..cubicTo(
        center - (direction * length * 0.14),
        y + (side * depth * 1.02),
        center - (direction * length * 0.05),
        y + (side * depth * 1.16),
        center,
        y + (side * depth * 1.16),
      )
      ..cubicTo(
        center + (direction * length * 0.05),
        y + (side * depth * 1.16),
        center + (direction * length * 0.14),
        y + (side * depth * 1.02),
        center + (direction * length * 0.16),
        y + (side * depth * 0.46),
      )
      ..cubicTo(
        center + (direction * length * 0.18),
        y + (side * depth * 0.06),
        connectorEnd - (direction * length * 0.05),
        y,
        connectorEnd,
        y,
      )
      ..lineTo(end.dx, end.dy);
  }

  void _drawVerticalEdge(
    Path path, {
    required Offset start,
    required Offset end,
    required PuzzleConnector connector,
    required double depth,
    required double outwardSign,
  }) {
    if (connector == PuzzleConnector.flat) {
      path.lineTo(end.dx, end.dy);
      return;
    }

    final direction = end.dy > start.dy ? 1.0 : -1.0;
    final length = (end.dy - start.dy).abs();
    final connectorStart = start.dy + (direction * length * 0.32);
    final connectorEnd = start.dy + (direction * length * 0.68);
    final center = start.dy + (direction * length * 0.50);
    final side = connector == PuzzleConnector.knob ? outwardSign : -outwardSign;
    final x = start.dx;

    path
      ..lineTo(x, connectorStart)
      ..cubicTo(
        x,
        connectorStart + (direction * length * 0.05),
        x + (side * depth * 0.06),
        center - (direction * length * 0.18),
        x + (side * depth * 0.46),
        center - (direction * length * 0.16),
      )
      ..cubicTo(
        x + (side * depth * 1.02),
        center - (direction * length * 0.14),
        x + (side * depth * 1.16),
        center - (direction * length * 0.05),
        x + (side * depth * 1.16),
        center,
      )
      ..cubicTo(
        x + (side * depth * 1.16),
        center + (direction * length * 0.05),
        x + (side * depth * 1.02),
        center + (direction * length * 0.14),
        x + (side * depth * 0.46),
        center + (direction * length * 0.16),
      )
      ..cubicTo(
        x + (side * depth * 0.06),
        center + (direction * length * 0.18),
        x,
        connectorEnd - (direction * length * 0.05),
        x,
        connectorEnd,
      )
      ..lineTo(end.dx, end.dy);
  }

  @override
  bool shouldReclip(RoundedPuzzlePieceClipper oldClipper) {
    return edges != oldClipper.edges ||
        connectorSize != oldClipper.connectorSize;
  }
}

class PuzzlePieceBorderPainter extends CustomPainter {
  const PuzzlePieceBorderPainter({
    required this.clipper,
    required this.color,
  });

  final RoundedPuzzlePieceClipper clipper;
  final Color color;

  @override
  void paint(Canvas canvas, Size size) {
    final path = clipper.getClip(size);
    final paint = Paint()
      ..color = color
      ..style = PaintingStyle.stroke
      ..strokeWidth = 2.4;

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
