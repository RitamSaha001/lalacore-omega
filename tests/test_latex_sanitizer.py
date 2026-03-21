import unittest

from latex_sanitizer import sanitize_latex, validate_latex


class LatexSanitizerTests(unittest.TestCase):
    def test_odd_dollar_sign_is_closed(self) -> None:
        text = "Find $x^2 + 1"
        out = sanitize_latex(text)
        self.assertEqual(out.count("$") % 2, 0)
        self.assertTrue(validate_latex(out))

    def test_broken_fraction_is_normalized(self) -> None:
        text = "Evaluate $1/2 + 1/3$"
        out = sanitize_latex(text)
        self.assertIn(r"\frac{1}{2}", out)
        self.assertIn(r"\frac{1}{3}", out)

    def test_unicode_minus_is_replaced(self) -> None:
        text = "Compute $5 − 2$"
        out = sanitize_latex(text)
        self.assertNotIn("−", out)
        self.assertIn("-", out)

    def test_nested_braces_are_balanced(self) -> None:
        text = "Simplify $\\frac{a+b}{c$"
        out = sanitize_latex(text)
        self.assertTrue(validate_latex(out))

    def test_forbidden_commands_are_removed(self) -> None:
        text = r"$x^2$ \\input{secrets} \\write18"
        out = sanitize_latex(text)
        self.assertNotIn(r"\input", out)
        self.assertNotIn(r"\write", out)
        self.assertTrue(validate_latex(out))


if __name__ == "__main__":
    unittest.main()
