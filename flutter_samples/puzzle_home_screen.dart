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
    final groupSizeByWidth = width * 0.98;
    final groupSizeByHeight = height * 0.66;
    final groupSize = groupSizeByWidth < groupSizeByHeight
        ? groupSizeByWidth
        : groupSizeByHeight;
    final connectorSpace = groupSize * 0.07;
    final pieceSize = (groupSize + (connectorSpace * 2)) / 2;
    final step = pieceSize - (connectorSpace * 2);
    final groupWidth = step + pieceSize;
    final fifthSize = groupSize * 0.40;
    final fullHeight = groupWidth + fifthSize + connectorSpace;
    final groupLeft = ((width - groupWidth) / 2).clamp(0.0, width).toDouble();
    final groupTop = ((height - fullHeight) / 2)
        .clamp(0.0, height - groupWidth)
        .toDouble();
    final fifthLeft = (groupLeft + groupWidth - (fifthSize * 0.95))
        .clamp(0.0, width - fifthSize)
        .toDouble();
    final fifthTop = groupTop + groupWidth + (connectorSpace * 0.40);

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
              icon: '🕐',
              label: 'Korvi ajalugu',
              color: const Color(0xFF5CB85C),
              edges: const PuzzlePieceEdges(
                top: PuzzleConnector.socket,
                right: PuzzleConnector.socket,
                bottom: PuzzleConnector.socket,
                left: PuzzleConnector.socket,
              ),
              size: pieceSize,
              connectorSpace: connectorSpace,
              onPressed: onHistory,
            ),
          ),
          Positioned(
            left: groupLeft + step,
            top: groupTop,
            child: PuzzlePieceButton(
              icon: '🛒',
              label: 'Sirvi tooteid',
              color: const Color(0xFF2196F3),
              edges: const PuzzlePieceEdges(
                top: PuzzleConnector.socket,
                right: PuzzleConnector.knob,
                bottom: PuzzleConnector.knob,
                left: PuzzleConnector.socket,
              ),
              size: pieceSize,
              connectorSpace: connectorSpace,
              onPressed: onProducts,
            ),
          ),
          Positioned(
            left: groupLeft,
            top: groupTop,
            child: PuzzlePieceButton(
              icon: '📊',
              label: 'Võrdle korvi',
              color: const Color(0xFFE8114B),
              edges: const PuzzlePieceEdges(
                top: PuzzleConnector.socket,
                right: PuzzleConnector.knob,
                bottom: PuzzleConnector.socket,
                left: PuzzleConnector.socket,
              ),
              size: pieceSize,
              connectorSpace: connectorSpace,
              onPressed: onCompare,
            ),
          ),
          Positioned(
            left: groupLeft,
            top: groupTop + step,
            child: PuzzlePieceButton(
              icon: '🧺',
              label: 'Ostukorv${itemCount > 0 ? " ($itemCount)" : ""}',
              color: const Color(0xFFFFB703),
              edges: const PuzzlePieceEdges(
                top: PuzzleConnector.knob,
                right: PuzzleConnector.knob,
                bottom: PuzzleConnector.socket,
                left: PuzzleConnector.knob,
              ),
              size: pieceSize,
              connectorSpace: connectorSpace,
              onPressed: onBasket,
            ),
          ),
          Positioned(
            left: fifthLeft,
            top: fifthTop,
            child: Transform.rotate(
              angle: -0.16,
              child: PuzzlePieceButton(
                icon: '🍲',
                label: 'Retseptid',
                color: Colors.white,
                foregroundColor: const Color(0xFF1A1A1A),
                borderColor: const Color(0x66808080),
                edges: const PuzzlePieceEdges(
                  top: PuzzleConnector.knob,
                  right: PuzzleConnector.knob,
                  bottom: PuzzleConnector.socket,
                  left: PuzzleConnector.socket,
                ),
                size: fifthSize,
                connectorSpace: fifthSize * 0.16,
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
    required this.connectorSpace,
    required this.onPressed,
    this.foregroundColor = Colors.white,
    this.borderColor = const Color(0x55FFFFFF),
  });

  final String icon;
  final String label;
  final Color color;
  final Color foregroundColor;
  final Color borderColor;
  final PuzzlePieceEdges edges;
  final double size;
  final double connectorSpace;
  final VoidCallback onPressed;

  @override
  Widget build(BuildContext context) {
    final clipper = PuzzlePieceClipper(
      edges: edges,
      connectorSpace: connectorSpace,
    );
    final filledSize = size - (connectorSpace * 2);
    final labelStyle = TextStyle(
      color: foregroundColor,
      fontWeight: FontWeight.w800,
      fontSize: (size * 0.085).clamp(14.0, 18.0).toDouble(),
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
              child: Material(
                color: Colors.transparent,
                child: InkWell(
                  onTap: onPressed,
                  splashColor: _alphaColor(foregroundColor, 0.22),
                  highlightColor: _alphaColor(foregroundColor, 0.10),
                  child: Stack(
                    children: [
                      Positioned(
                        left: connectorSpace + (filledSize * 0.08),
                        top: connectorSpace + (filledSize * 0.34),
                        width: filledSize * 0.84,
                        height: filledSize * 0.32,
                        child: FittedBox(
                          fit: BoxFit.scaleDown,
                          child: Row(
                            mainAxisSize: MainAxisSize.min,
                            children: [
                              Text(
                                icon,
                                style: TextStyle(
                                  fontSize: (size * 0.13)
                                      .clamp(22.0, 32.0)
                                      .toDouble(),
                                ),
                              ),
                              SizedBox(width: size * 0.035),
                              Text(
                                label,
                                maxLines: 1,
                                overflow: TextOverflow.visible,
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

class PuzzlePieceClipper extends CustomClipper<Path> {
  const PuzzlePieceClipper({
    required this.edges,
    required this.connectorSpace,
  });

  final PuzzlePieceEdges edges;
  final double connectorSpace;

  @override
  Path getClip(Size size) {
    final rect = Rect.fromLTWH(
      connectorSpace,
      connectorSpace,
      size.width - (connectorSpace * 2),
      size.height - (connectorSpace * 2),
    );
    final depth = connectorSpace * 1.10;

    final path = Path()..moveTo(rect.left, rect.top);

    _drawTopEdge(path, rect, edges.top, depth);
    _drawRightEdge(path, rect, edges.right, depth);
    _drawBottomEdge(path, rect, edges.bottom, depth);
    _drawLeftEdge(path, rect, edges.left, depth);

    return path..close();
  }

  void _drawTopEdge(
    Path path,
    Rect rect,
    PuzzleConnector connector,
    double depth,
  ) {
    if (connector == PuzzleConnector.flat) {
      path.lineTo(rect.right, rect.top);
      return;
    }

    final start = rect.left + rect.width * 0.30;
    final end = rect.left + rect.width * 0.70;
    final center = rect.center.dx;
    final direction = connector == PuzzleConnector.knob ? -1.0 : 1.0;

    path
      ..lineTo(start, rect.top)
      ..cubicTo(
        start + rect.width * 0.05,
        rect.top,
        center - rect.width * 0.18,
        rect.top + (direction * depth * 0.10),
        center - rect.width * 0.17,
        rect.top + (direction * depth * 0.52),
      )
      ..cubicTo(
        center - rect.width * 0.16,
        rect.top + (direction * depth * 1.08),
        center - rect.width * 0.06,
        rect.top + (direction * depth * 1.18),
        center,
        rect.top + (direction * depth * 1.18),
      )
      ..cubicTo(
        center + rect.width * 0.06,
        rect.top + (direction * depth * 1.18),
        center + rect.width * 0.16,
        rect.top + (direction * depth * 1.08),
        center + rect.width * 0.17,
        rect.top + (direction * depth * 0.52),
      )
      ..cubicTo(
        center + rect.width * 0.18,
        rect.top + (direction * depth * 0.10),
        end - rect.width * 0.05,
        rect.top,
        end,
        rect.top,
      )
      ..lineTo(rect.right, rect.top);
  }

  void _drawRightEdge(
    Path path,
    Rect rect,
    PuzzleConnector connector,
    double depth,
  ) {
    if (connector == PuzzleConnector.flat) {
      path.lineTo(rect.right, rect.bottom);
      return;
    }

    final start = rect.top + rect.height * 0.30;
    final end = rect.top + rect.height * 0.70;
    final center = rect.center.dy;
    final direction = connector == PuzzleConnector.knob ? 1.0 : -1.0;

    path
      ..lineTo(rect.right, start)
      ..cubicTo(
        rect.right,
        start + rect.height * 0.05,
        rect.right + (direction * depth * 0.10),
        center - rect.height * 0.18,
        rect.right + (direction * depth * 0.52),
        center - rect.height * 0.17,
      )
      ..cubicTo(
        rect.right + (direction * depth * 1.08),
        center - rect.height * 0.16,
        rect.right + (direction * depth * 1.18),
        center - rect.height * 0.06,
        rect.right + (direction * depth * 1.18),
        center,
      )
      ..cubicTo(
        rect.right + (direction * depth * 1.18),
        center + rect.height * 0.06,
        rect.right + (direction * depth * 1.08),
        center + rect.height * 0.16,
        rect.right + (direction * depth * 0.52),
        center + rect.height * 0.17,
      )
      ..cubicTo(
        rect.right + (direction * depth * 0.10),
        center + rect.height * 0.18,
        rect.right,
        end - rect.height * 0.05,
        rect.right,
        end,
      )
      ..lineTo(rect.right, rect.bottom);
  }

  void _drawBottomEdge(
    Path path,
    Rect rect,
    PuzzleConnector connector,
    double depth,
  ) {
    if (connector == PuzzleConnector.flat) {
      path.lineTo(rect.left, rect.bottom);
      return;
    }

    final start = rect.right - rect.width * 0.30;
    final end = rect.left + rect.width * 0.30;
    final center = rect.center.dx;
    final direction = connector == PuzzleConnector.knob ? 1.0 : -1.0;

    path
      ..lineTo(start, rect.bottom)
      ..cubicTo(
        start - rect.width * 0.05,
        rect.bottom,
        center + rect.width * 0.18,
        rect.bottom + (direction * depth * 0.10),
        center + rect.width * 0.17,
        rect.bottom + (direction * depth * 0.52),
      )
      ..cubicTo(
        center + rect.width * 0.16,
        rect.bottom + (direction * depth * 1.08),
        center + rect.width * 0.06,
        rect.bottom + (direction * depth * 1.18),
        center,
        rect.bottom + (direction * depth * 1.18),
      )
      ..cubicTo(
        center - rect.width * 0.06,
        rect.bottom + (direction * depth * 1.18),
        center - rect.width * 0.16,
        rect.bottom + (direction * depth * 1.08),
        center - rect.width * 0.17,
        rect.bottom + (direction * depth * 0.52),
      )
      ..cubicTo(
        center - rect.width * 0.18,
        rect.bottom + (direction * depth * 0.10),
        end + rect.width * 0.05,
        rect.bottom,
        end,
        rect.bottom,
      )
      ..lineTo(rect.left, rect.bottom);
  }

  void _drawLeftEdge(
    Path path,
    Rect rect,
    PuzzleConnector connector,
    double depth,
  ) {
    if (connector == PuzzleConnector.flat) {
      path.lineTo(rect.left, rect.top);
      return;
    }

    final start = rect.bottom - rect.height * 0.30;
    final end = rect.top + rect.height * 0.30;
    final center = rect.center.dy;
    final direction = connector == PuzzleConnector.knob ? -1.0 : 1.0;

    path
      ..lineTo(rect.left, start)
      ..cubicTo(
        rect.left,
        start - rect.height * 0.05,
        rect.left + (direction * depth * 0.10),
        center + rect.height * 0.18,
        rect.left + (direction * depth * 0.52),
        center + rect.height * 0.17,
      )
      ..cubicTo(
        rect.left + (direction * depth * 1.08),
        center + rect.height * 0.16,
        rect.left + (direction * depth * 1.18),
        center + rect.height * 0.06,
        rect.left + (direction * depth * 1.18),
        center,
      )
      ..cubicTo(
        rect.left + (direction * depth * 1.18),
        center - rect.height * 0.06,
        rect.left + (direction * depth * 1.08),
        center - rect.height * 0.16,
        rect.left + (direction * depth * 0.52),
        center - rect.height * 0.17,
      )
      ..cubicTo(
        rect.left + (direction * depth * 0.10),
        center - rect.height * 0.18,
        rect.left,
        end + rect.height * 0.05,
        rect.left,
        end,
      )
      ..lineTo(rect.left, rect.top);
  }

  @override
  bool shouldReclip(PuzzlePieceClipper oldClipper) {
    return edges != oldClipper.edges ||
        connectorSpace != oldClipper.connectorSpace;
  }
}

class PuzzlePieceBorderPainter extends CustomPainter {
  const PuzzlePieceBorderPainter({
    required this.clipper,
    required this.color,
  });

  final PuzzlePieceClipper clipper;
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
