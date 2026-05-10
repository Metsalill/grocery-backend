import 'package:flutter/material.dart';

void main() {
  runApp(const PuzzleButtonsDemoApp());
}

class PuzzleButtonsDemoApp extends StatelessWidget {
  const PuzzleButtonsDemoApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xff1769aa)),
        useMaterial3: true,
      ),
      home: const PuzzleHomeScreen(),
    );
  }
}

class PuzzleHomeScreen extends StatelessWidget {
  const PuzzleHomeScreen({
    super.key,
    this.onPiecePressed,
  });

  final ValueChanged<int>? onPiecePressed;

  static const double _pieceSize = 250;
  static const double _connectorSpace = 40;
  static const double _baseSpan = _pieceSize - (_connectorSpace * 2);

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xffeef3f8),
      body: SafeArea(
        child: Padding(
          padding: const EdgeInsets.all(24),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              Text(
                'Choose a puzzle piece',
                textAlign: TextAlign.center,
                style: Theme.of(context).textTheme.headlineMedium?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
              ),
              const SizedBox(height: 24),
              Expanded(
                child: Center(
                  child: FittedBox(
                    fit: BoxFit.contain,
                    child: SizedBox(
                      width: 720,
                      height: 570,
                      child: Stack(
                        clipBehavior: Clip.none,
                        children: [
                          Positioned(
                            left: 0,
                            top: 0,
                            child: PuzzlePieceButton(
                              label: '1',
                              color: const Color(0xffe91e63),
                              edges: const PuzzlePieceEdges(
                                top: PuzzleConnector.flat,
                                right: PuzzleConnector.knob,
                                bottom: PuzzleConnector.socket,
                                left: PuzzleConnector.flat,
                              ),
                              onPressed: () => _handlePiecePressed(context, 1),
                            ),
                          ),
                          Positioned(
                            left: _baseSpan,
                            top: 0,
                            child: PuzzlePieceButton(
                              label: '2',
                              color: const Color(0xff1565c0),
                              edges: const PuzzlePieceEdges(
                                top: PuzzleConnector.flat,
                                right: PuzzleConnector.knob,
                                bottom: PuzzleConnector.knob,
                                left: PuzzleConnector.socket,
                              ),
                              onPressed: () => _handlePiecePressed(context, 2),
                            ),
                          ),
                          Positioned(
                            left: 0,
                            top: _baseSpan,
                            child: PuzzlePieceButton(
                              label: '3',
                              color: const Color(0xffffca28),
                              edges: const PuzzlePieceEdges(
                                top: PuzzleConnector.knob,
                                right: PuzzleConnector.knob,
                                bottom: PuzzleConnector.socket,
                                left: PuzzleConnector.knob,
                              ),
                              onPressed: () => _handlePiecePressed(context, 3),
                            ),
                          ),
                          Positioned(
                            left: _baseSpan,
                            top: _baseSpan,
                            child: PuzzlePieceButton(
                              label: '4',
                              color: const Color(0xff52c600),
                              edges: const PuzzlePieceEdges(
                                top: PuzzleConnector.socket,
                                right: PuzzleConnector.socket,
                                bottom: PuzzleConnector.socket,
                                left: PuzzleConnector.socket,
                              ),
                              onPressed: () => _handlePiecePressed(context, 4),
                            ),
                          ),
                          Positioned(
                            left: 430,
                            top: 280,
                            child: Transform.rotate(
                              angle: -0.18,
                              child: PuzzlePieceButton(
                                label: '5',
                                color: Colors.white,
                                foregroundColor: const Color(0xff263238),
                                borderColor: const Color(0xff9e9e9e),
                                edges: const PuzzlePieceEdges(
                                  top: PuzzleConnector.knob,
                                  right: PuzzleConnector.knob,
                                  bottom: PuzzleConnector.socket,
                                  left: PuzzleConnector.socket,
                                ),
                                onPressed: () =>
                                    _handlePiecePressed(context, 5),
                              ),
                            ),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  void _handlePiecePressed(BuildContext context, int pieceNumber) {
    onPiecePressed?.call(pieceNumber);

    if (onPiecePressed != null) {
      return;
    }

    ScaffoldMessenger.of(context)
      ..hideCurrentSnackBar()
      ..showSnackBar(
        SnackBar(content: Text('Puzzle piece $pieceNumber tapped')),
      );
  }
}

class PuzzlePieceButton extends StatelessWidget {
  const PuzzlePieceButton({
    super.key,
    required this.label,
    required this.color,
    required this.edges,
    required this.onPressed,
    this.foregroundColor = Colors.white,
    this.borderColor = const Color(0x55000000),
    this.size = 250,
    this.connectorSpace = 40,
  });

  final String label;
  final Color color;
  final Color foregroundColor;
  final Color borderColor;
  final PuzzlePieceEdges edges;
  final VoidCallback onPressed;
  final double size;
  final double connectorSpace;

  @override
  Widget build(BuildContext context) {
    final clipper = PuzzlePieceClipper(
      edges: edges,
      connectorSpace: connectorSpace,
    );

    return Semantics(
      button: true,
      label: 'Puzzle piece $label',
      child: SizedBox.square(
        dimension: size,
        child: Stack(
          children: [
            PhysicalShape(
              clipper: clipper,
              color: color,
              elevation: 8,
              shadowColor: Colors.black.withValues(alpha: 0.28),
              child: Material(
                color: Colors.transparent,
                child: InkWell(
                  onTap: onPressed,
                  splashColor: foregroundColor.withValues(alpha: 0.22),
                  highlightColor: foregroundColor.withValues(alpha: 0.10),
                  child: Center(
                    child: Text(
                      label,
                      style: Theme.of(context).textTheme.displaySmall?.copyWith(
                            color: foregroundColor,
                            fontWeight: FontWeight.w800,
                          ),
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
    final depth = connectorSpace * 0.95;

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

    final start = rect.left + rect.width * 0.34;
    final end = rect.left + rect.width * 0.66;
    final center = rect.center.dx;
    final direction = connector == PuzzleConnector.knob ? -1.0 : 1.0;

    path
      ..lineTo(start, rect.top)
      ..cubicTo(
        start + rect.width * 0.04,
        rect.top,
        center - rect.width * 0.20,
        rect.top + (direction * depth),
        center,
        rect.top + (direction * depth),
      )
      ..cubicTo(
        center + rect.width * 0.20,
        rect.top + (direction * depth),
        end - rect.width * 0.04,
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

    final start = rect.top + rect.height * 0.34;
    final end = rect.top + rect.height * 0.66;
    final center = rect.center.dy;
    final direction = connector == PuzzleConnector.knob ? 1.0 : -1.0;

    path
      ..lineTo(rect.right, start)
      ..cubicTo(
        rect.right,
        start + rect.height * 0.04,
        rect.right + (direction * depth),
        center - rect.height * 0.20,
        rect.right + (direction * depth),
        center,
      )
      ..cubicTo(
        rect.right + (direction * depth),
        center + rect.height * 0.20,
        rect.right,
        end - rect.height * 0.04,
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

    final start = rect.right - rect.width * 0.34;
    final end = rect.left + rect.width * 0.34;
    final center = rect.center.dx;
    final direction = connector == PuzzleConnector.knob ? 1.0 : -1.0;

    path
      ..lineTo(start, rect.bottom)
      ..cubicTo(
        start - rect.width * 0.04,
        rect.bottom,
        center + rect.width * 0.20,
        rect.bottom + (direction * depth),
        center,
        rect.bottom + (direction * depth),
      )
      ..cubicTo(
        center - rect.width * 0.20,
        rect.bottom + (direction * depth),
        end + rect.width * 0.04,
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

    final start = rect.bottom - rect.height * 0.34;
    final end = rect.top + rect.height * 0.34;
    final center = rect.center.dy;
    final direction = connector == PuzzleConnector.knob ? -1.0 : 1.0;

    path
      ..lineTo(rect.left, start)
      ..cubicTo(
        rect.left,
        start - rect.height * 0.04,
        rect.left + (direction * depth),
        center + rect.height * 0.20,
        rect.left + (direction * depth),
        center,
      )
      ..cubicTo(
        rect.left + (direction * depth),
        center - rect.height * 0.20,
        rect.left,
        end + rect.height * 0.04,
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
      ..strokeWidth = 2.4;

    canvas.drawPath(path, paint);
  }

  @override
  bool shouldRepaint(PuzzlePieceBorderPainter oldDelegate) {
    return clipper != oldDelegate.clipper || color != oldDelegate.color;
  }
}
