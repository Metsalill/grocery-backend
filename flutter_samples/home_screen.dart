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
    const connectorSourceSize = 34.0;
    const pieceGapSource = 7.0;
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

class PuzzlePieceButton extends StatefulWidget {
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
    this.borderColor = const Color(0xCC000000),
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
  State<PuzzlePieceButton> createState() => _PuzzlePieceButtonState();
}

class _PuzzlePieceButtonState extends State<PuzzlePieceButton> {
  bool _pressed = false;

  @override
  Widget build(BuildContext context) {
    final clipper = RoundedPuzzlePieceClipper(
      edges: widget.edges,
      connectorSize: widget.connectorSize,
    );
    final labelStyle = TextStyle(
      color: widget.foregroundColor,
      fontWeight: FontWeight.w900,
      fontSize: (widget.size * 0.068).clamp(16.0, 24.0).toDouble(),
      height: 1.04,
      letterSpacing: -0.45,
      shadows: [
        Shadow(
          color: _alphaColor(
            Colors.black,
            widget.color == Colors.white ? 0.0 : 0.28,
          ),
          blurRadius: 3,
          offset: const Offset(0, 1.4),
        ),
      ],
    );

    return Semantics(
      button: true,
      label: widget.label.replaceAll('\n', ' '),
      child: AnimatedScale(
        duration: const Duration(milliseconds: 90),
        curve: Curves.easeOut,
        scale: _pressed ? 0.975 : 1,
        child: AnimatedSlide(
          duration: const Duration(milliseconds: 90),
          curve: Curves.easeOut,
          offset: _pressed ? const Offset(0, 0.012) : Offset.zero,
          child: SizedBox.square(
            dimension: widget.size,
            child: Stack(
              children: [
                PhysicalShape(
                  clipper: clipper,
                  clipBehavior: Clip.antiAlias,
                  color: widget.color,
                  elevation: _pressed ? 1.5 : 4,
                  shadowColor: _alphaColor(
                    Colors.black,
                    _pressed ? 0.10 : 0.18,
                  ),
                  child: DecoratedBox(
                    decoration: BoxDecoration(
                      gradient: LinearGradient(
                        begin: Alignment.topLeft,
                        end: Alignment.bottomRight,
                        colors: [
                          _lightenColor(
                            widget.color,
                            widget.color == Colors.white ? 0.0 : 0.18,
                          ),
                          widget.color,
                          _darkenColor(
                            widget.color,
                            widget.color == Colors.white ? 0.04 : 0.10,
                          ),
                        ],
                      ),
                    ),
                    child: Material(
                      color: Colors.transparent,
                      child: InkWell(
                        onTap: widget.onPressed,
                        onHighlightChanged: (highlighted) {
                          setState(() => _pressed = highlighted);
                        },
                        splashColor: _alphaColor(widget.foregroundColor, 0.20),
                        highlightColor:
                            _alphaColor(widget.foregroundColor, 0.08),
                        child: Stack(
                          children: [
                            Positioned(
                              left: widget.size * 0.22,
                              top: widget.size * 0.28,
                              width: widget.size * 0.50,
                              height: widget.size * 0.46,
                              child: FittedBox(
                                fit: BoxFit.scaleDown,
                                child: Column(
                                  mainAxisSize: MainAxisSize.min,
                                  children: [
                                    Icon(
                                      widget.icon,
                                      color: widget.foregroundColor,
                                      size: (widget.size * 0.16)
                                          .clamp(30.0, 46.0)
                                          .toDouble(),
                                    ),
                                    SizedBox(height: widget.size * 0.012),
                                    Text(
                                      widget.label,
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
                        color: widget.borderColor,
                      ),
                    ),
                  ),
                ),
              ],
            ),
          ),
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
    final depth = connectorSize * 1.16;

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

    final length = (end.dx - start.dx).abs();
    final left = start.dx < end.dx ? start.dx : end.dx;
    final connectorStart = left + (length * 0.32);
    final connectorEnd = left + (length * 0.68);
    final connectorLength = connectorEnd - connectorStart;
    final side = connector == PuzzleConnector.knob ? outwardSign : -outwardSign;
    final y = start.dy;

    double xAt(double t) => connectorStart + (connectorLength * t);
    double yAt(double d) => y + (side * depth * d);

    final forward = end.dx > start.dx;

    if (forward) {
      path
        ..lineTo(connectorStart, y)
        ..cubicTo(
          xAt(0.10),
          yAt(0.00),
          xAt(0.16),
          yAt(0.14),
          xAt(0.10),
          yAt(0.28),
        )
        ..cubicTo(
          xAt(0.02),
          yAt(0.44),
          xAt(0.03),
          yAt(0.64),
          xAt(0.14),
          yAt(0.80),
        )
        ..cubicTo(
          xAt(0.27),
          yAt(1.00),
          xAt(0.53),
          yAt(1.08),
          xAt(0.76),
          yAt(0.96),
        )
        ..cubicTo(
          xAt(0.91),
          yAt(0.89),
          xAt(0.95),
          yAt(0.73),
          xAt(0.87),
          yAt(0.54),
        )
        ..cubicTo(
          xAt(0.79),
          yAt(0.35),
          xAt(0.82),
          yAt(0.00),
          connectorEnd,
          y,
        )
        ..lineTo(end.dx, end.dy);
      return;
    }

    path
      ..lineTo(connectorEnd, y)
      ..cubicTo(
        xAt(0.82),
        yAt(0.00),
        xAt(0.79),
        yAt(0.35),
        xAt(0.87),
        yAt(0.54),
      )
      ..cubicTo(
        xAt(0.95),
        yAt(0.73),
        xAt(0.91),
        yAt(0.89),
        xAt(0.76),
        yAt(0.96),
      )
      ..cubicTo(
        xAt(0.53),
        yAt(1.08),
        xAt(0.27),
        yAt(1.00),
        xAt(0.14),
        yAt(0.80),
      )
      ..cubicTo(
        xAt(0.03),
        yAt(0.64),
        xAt(0.02),
        yAt(0.44),
        xAt(0.10),
        yAt(0.28),
      )
      ..cubicTo(
        xAt(0.16),
        yAt(0.14),
        xAt(0.10),
        yAt(0.00),
        connectorStart,
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

    final length = (end.dy - start.dy).abs();
    final top = start.dy < end.dy ? start.dy : end.dy;
    final connectorStart = top + (length * 0.32);
    final connectorEnd = top + (length * 0.68);
    final connectorLength = connectorEnd - connectorStart;
    final side = connector == PuzzleConnector.knob ? outwardSign : -outwardSign;
    final x = start.dx;

    double xAt(double d) => x + (side * depth * d);
    double yAt(double t) => connectorStart + (connectorLength * t);

    final forward = end.dy > start.dy;

    if (forward) {
      path
        ..lineTo(x, connectorStart)
        ..cubicTo(
          x,
          yAt(0.10),
          xAt(0.14),
          yAt(0.16),
          xAt(0.28),
          yAt(0.10),
        )
        ..cubicTo(
          xAt(0.44),
          yAt(0.02),
          xAt(0.64),
          yAt(0.03),
          xAt(0.80),
          yAt(0.14),
        )
        ..cubicTo(
          xAt(1.00),
          yAt(0.27),
          xAt(1.08),
          yAt(0.53),
          xAt(0.96),
          yAt(0.76),
        )
        ..cubicTo(
          xAt(0.89),
          yAt(0.91),
          xAt(0.73),
          yAt(0.95),
          xAt(0.54),
          yAt(0.87),
        )
        ..cubicTo(
          xAt(0.35),
          yAt(0.79),
          x,
          yAt(0.82),
          x,
          connectorEnd,
        )
        ..lineTo(end.dx, end.dy);
      return;
    }

    path
      ..lineTo(x, connectorEnd)
      ..cubicTo(
        x,
        yAt(0.82),
        xAt(0.35),
        yAt(0.79),
        xAt(0.54),
        yAt(0.87),
      )
      ..cubicTo(
        xAt(0.73),
        yAt(0.95),
        xAt(0.89),
        yAt(0.91),
        xAt(0.96),
        yAt(0.76),
      )
      ..cubicTo(
        xAt(1.08),
        yAt(0.53),
        xAt(1.00),
        yAt(0.27),
        xAt(0.80),
        yAt(0.14),
      )
      ..cubicTo(
        xAt(0.64),
        yAt(0.03),
        xAt(0.44),
        yAt(0.02),
        xAt(0.28),
        yAt(0.10),
      )
      ..cubicTo(
        xAt(0.14),
        yAt(0.16),
        x,
        yAt(0.10),
        x,
        connectorStart,
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
      ..strokeWidth = 1.45;

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
