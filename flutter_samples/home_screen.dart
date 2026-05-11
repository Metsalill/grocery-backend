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

  void _showRecipesComingSoon() {
    ScaffoldMessenger.of(context)
      ..hideCurrentSnackBar()
      ..showSnackBar(const SnackBar(content: Text('Retseptid tulekul!')));
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
        child: SingleChildScrollView(
          padding: const EdgeInsets.fromLTRB(16, 8, 16, 24),
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
              const SizedBox(height: 16),
              LayoutBuilder(
                builder: (context, constraints) {
                  final buttonSize = (constraints.maxWidth - 12) / 2;
                  return Column(
                    children: [
                      Row(
                        children: [
                          _squareButton(
                            buttonSize,
                            Icons.insert_chart_rounded,
                            'Võrdle korvi',
                            const Color(0xFFE91E63),
                            Colors.white,
                            _navigateToCompare,
                          ),
                          const SizedBox(width: 12),
                          _squareButton(
                            buttonSize,
                            Icons.shopping_cart_rounded,
                            'Sirvi tooteid',
                            const Color(0xFF1476C9),
                            Colors.white,
                            _navigateToProducts,
                          ),
                        ],
                      ),
                      const SizedBox(height: 12),
                      Row(
                        children: [
                          _squareButton(
                            buttonSize,
                            Icons.shopping_basket_rounded,
                            itemCount > 0 ? 'Ostukorv ($itemCount)' : 'Ostukorv',
                            const Color(0xFFFFB703),
                            const Color(0xFF1A1A1A),
                            _navigateToBasket,
                          ),
                          const SizedBox(width: 12),
                          _squareButton(
                            buttonSize,
                            Icons.history_rounded,
                            'Korvi ajalugu',
                            const Color(0xFF55C600),
                            Colors.white,
                            _navigateToBasketHistory,
                          ),
                        ],
                      ),
                    ],
                  );
                },
              ),
              const SizedBox(height: 18),
              _recipeSearchSection(),
            ],
          ),
        ),
      ),
    );
  }

  Widget _squareButton(
    double size,
    IconData icon,
    String label,
    Color color,
    Color fg,
    VoidCallback onPressed,
  ) {
    return SizedBox(
      width: size,
      height: size,
      child: Material(
        color: color,
        borderRadius: BorderRadius.circular(20),
        elevation: 3,
        shadowColor: color.withAlpha(80),
        child: InkWell(
          onTap: onPressed,
          borderRadius: BorderRadius.circular(20),
          splashColor: Colors.white24,
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Icon(icon, color: fg, size: 40),
              const SizedBox(height: 10),
              Padding(
                padding: const EdgeInsets.symmetric(horizontal: 10),
                child: Text(
                  label,
                  textAlign: TextAlign.center,
                  style: TextStyle(
                    color: fg,
                    fontWeight: FontWeight.w800,
                    fontSize: 15,
                    height: 1.2,
                  ),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _recipeSearchSection() {
    const recipes = [
      'Kana-riisi kauss',
      'Tomatine pasta juustuga',
      'Ahjukartul köögiviljadega',
    ];

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Material(
          color: Colors.white,
          borderRadius: BorderRadius.circular(20),
          elevation: 2,
          shadowColor: Colors.black.withAlpha(24),
          child: InkWell(
            onTap: _showRecipesComingSoon,
            borderRadius: BorderRadius.circular(20),
            child: Container(
              height: 56,
              padding: const EdgeInsets.symmetric(horizontal: 16),
              child: Row(
                children: [
                  Icon(Icons.search_rounded, color: Colors.grey.shade700),
                  const SizedBox(width: 10),
                  Expanded(
                    child: Text(
                      'Otsi retsepte...',
                      style: TextStyle(
                        color: Colors.grey.shade600,
                        fontSize: 16,
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                  ),
                  const Icon(
                    Icons.restaurant_menu_rounded,
                    color: Color(0xFFE91E63),
                  ),
                ],
              ),
            ),
          ),
        ),
        const SizedBox(height: 14),
        const Text(
          'Populaarsed retseptid',
          style: TextStyle(
            color: Color(0xFF1A1A1A),
            fontSize: 17,
            fontWeight: FontWeight.w900,
            letterSpacing: -0.2,
          ),
        ),
        const SizedBox(height: 10),
        for (var i = 0; i < recipes.length; i++) ...[
          _recipeSuggestionTile(i + 1, recipes[i]),
          if (i != recipes.length - 1) const SizedBox(height: 8),
        ],
      ],
    );
  }

  Widget _recipeSuggestionTile(int index, String title) {
    return Material(
      color: Colors.white,
      borderRadius: BorderRadius.circular(16),
      child: InkWell(
        onTap: _showRecipesComingSoon,
        borderRadius: BorderRadius.circular(16),
        child: Container(
          height: 48,
          padding: const EdgeInsets.symmetric(horizontal: 12),
          child: Row(
            children: [
              Container(
                width: 28,
                height: 28,
                alignment: Alignment.center,
                decoration: const BoxDecoration(
                  color: Color(0xFFFFB703),
                  shape: BoxShape.circle,
                ),
                child: Text(
                  '$index',
                  style: const TextStyle(
                    color: Color(0xFF1A1A1A),
                    fontWeight: FontWeight.w900,
                  ),
                ),
              ),
              const SizedBox(width: 10),
              Expanded(
                child: Text(
                  title,
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(
                    color: Color(0xFF1A1A1A),
                    fontWeight: FontWeight.w800,
                    fontSize: 14.5,
                  ),
                ),
              ),
              const Icon(Icons.chevron_right_rounded, color: Color(0xFF77716B)),
            ],
          ),
        ),
      ),
    );
  }
}
