import unittest

from risk_monitor.heuristics import (
    default_hazard_focus,
    nearest_region,
    outlook_heuristic,
    parse_outlook_json,
    trend_label,
)


class TestHeuristics(unittest.TestCase):
    def test_trend(self):
        self.assertIn("Up", trend_label(2, 1))
        self.assertIn("Down", trend_label(1, 3))
        self.assertIn("Stable", trend_label(2, 2))

    def test_outlook_heuristic(self):
        r = {"a": {"percentile_approx": 40.0}}
        f = {"a": {"percentile_approx": 50.0}}
        self.assertIn("worsening", outlook_heuristic(r, f, "→ Stable"))
        r2 = {"a": {"percentile_approx": 50.0}}
        f2 = {"a": {"percentile_approx": 40.0}}
        self.assertIn("improving", outlook_heuristic(r2, f2, "→ Stable"))

    def test_default_hazard_focus(self):
        scores = {"flood": 2, "fire": 2, "drought": 0, "landslide": 0}
        self.assertEqual(default_hazard_focus(scores, ("flood", "fire", "drought", "landslide")), "flood")

    def test_nearest_region(self):
        regs = [
            {"region": "A", "centroid_lat": 0.0, "centroid_lon": 0.0},
            {"region": "B", "centroid_lat": 10.0, "centroid_lon": 10.0},
        ]
        self.assertEqual(nearest_region(0.1, 0.1, regs), "A")

    def test_parse_outlook_json(self):
        self.assertEqual(parse_outlook_json("{}"), {})
        self.assertEqual(parse_outlook_json('{"x": 1}'), {"x": 1})


if __name__ == "__main__":
    unittest.main()
