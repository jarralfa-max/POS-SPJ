# core/services/enterprise/loyalty_enterprise_engine.py
# ── LoyaltyEnterpriseEngine — 4-Layer Loyalty System ─────────────────────────
# Layer 1: Individual Points (with dynamic multipliers, margin guard)
# Layer 2: Levels / Status (Bronze, Silver, Gold, Platinum)
# Layer 3: Challenges (Gamification with progress tracking)
# Layer 4: Community Goals (collective progress)
#
# Financial protections:
#   - Margin validation before reward
#   - Monthly budget cap
#   - Dynamic multiplier adjustment
#   - Max discount per sale
#   - Redemption ceiling
#   - ROI tracking
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("spj.loyalty.enterprise")

# ── Constants ─────────────────────────────────────────────────────────────────

LEVEL_BRONZE   = "Bronce"
LEVEL_SILVER   = "Plata"
LEVEL_GOLD     = "Oro"
LEVEL_PLATINUM = "Platino"

LEVEL_THRESHOLDS = {
    LEVEL_BRONZE:   0,
    LEVEL_SILVER:   40,
    LEVEL_GOLD:     65,
    LEVEL_PLATINUM: 85,
}

LEVEL_ORDER = [LEVEL_BRONZE, LEVEL_SILVER, LEVEL_GOLD, LEVEL_PLATINUM]


# ── DTOs ──────────────────────────────────────────────────────────────────────

@dataclass
class LoyaltyEarnResult:
    cliente_id:       int
    points_earned:    int
    points_total:     int
    multiplier_used:  float
    level_before:     str
    level_after:      str
    level_up:         bool
    budget_remaining: float
    challenges_updated: List[int]
    community_updated:  List[int]
    ticket_messages:  List[str]
    roi_logged:       bool


@dataclass
class LoyaltyRedeemResult:
    cliente_id:    int
    points_before: int
    points_redeemed: int
    points_after:  int
    discount_value: float
    allowed:       bool
    reason:        str


@dataclass
class TicketEngagementData:
    points_earned:      int
    points_total:       int
    level:              str
    pts_to_next_level:  int
    next_level:         str
    community_goal_name: Optional[str]
    community_pct:      float
    challenge_name:     Optional[str]
    challenge_progress: float
    messages:           List[str]


# ── Engine ────────────────────────────────────────────────────────────────────

class LoyaltyEnterpriseEngine:

    def __init__(self, db):
        self.db = db

    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    def _today(self) -> str:
        return date.today().isoformat()

    def _year_month(self) -> str:
        return datetime.utcnow().strftime("%Y-%m")

    # ── Configuration ─────────────────────────────────────────────────────────

    def _get_constant(self, key: str, default: str) -> str:
        row = self.db.fetchone(
            "SELECT value FROM system_constants WHERE key = ?", (key,)
        )
        return row["value"] if row else default

    def _get_float_constant(self, key: str, default: float) -> float:
        try:
            return float(self._get_constant(key, str(default)))
        except (TypeError, ValueError):
            return default

    def _get_int_constant(self, key: str, default: int) -> int:
        try:
            return int(float(self._get_constant(key, str(default))))
        except (TypeError, ValueError):
            return default

    def _get_redemption_limits(self, branch_id: int) -> Dict:
        row = self.db.fetchone("""
            SELECT max_pct_per_sale, max_pts_per_sale,
                   max_monthly_pts, min_purchase_for_points
            FROM loyalty_redemption_limits WHERE branch_id = ?
        """, (branch_id,))
        if row:
            return dict(row)
        return {
            "max_pct_per_sale": self._get_float_constant("LOYALTY_MAX_DISCOUNT_PCT", 30.0),
            "max_pts_per_sale": 500,
            "max_monthly_pts":  5000,
            "min_purchase_for_points": 50.0,
        }

    # ── Layer 1: Points Earning ───────────────────────────────────────────────

    def earn_points(self, cliente_id: int, sale_total: float,
                    margin_real: float, branch_id: int,
                    sale_id: int, items: List[Dict]) -> LoyaltyEarnResult:
        """
        Award points for a completed sale with all financial guards.
        margin_real: actual margin [0..1] for this sale.
        """
        margin_floor = self._get_float_constant("LOYALTY_MARGIN_FLOOR", 0.15)
        pts_per_peso = self._get_float_constant("LOYALTY_POINTS_PER_PESO", 1.0)
        min_purchase = self._get_redemption_limits(branch_id)["min_purchase_for_points"]

        # Guard 1: minimum purchase
        if sale_total < min_purchase:
            return self._empty_result(cliente_id, "BELOW_MIN_PURCHASE")

        # Guard 2: margin floor — no rewards for loss-making sales
        if margin_real < margin_floor:
            return self._empty_result(cliente_id, "BELOW_MARGIN_FLOOR")

        # Get client state
        client_row = self.db.fetchone("""
            SELECT puntos, nivel_fidelidad
            FROM clientes WHERE id = ?
        """, (cliente_id,))
        if not client_row:
            return self._empty_result(cliente_id, "CLIENTE_NOT_FOUND")

        pts_before   = int(client_row["puntos"] or 0)
        level_before = client_row["nivel_fidelidad"] or LEVEL_BRONZE

        # Guard 3: monthly budget cap
        budget_remaining = self._check_budget_cap(branch_id, sale_total, pts_per_peso)
        if budget_remaining <= 0:
            return self._empty_result(cliente_id, "BUDGET_CAP_REACHED")

        # Compute multiplier
        multiplier = self._compute_multiplier(
            cliente_id, level_before, branch_id, items
        )

        # Compute raw points
        raw_points = int(
            Decimal(str(sale_total)) *
            Decimal(str(pts_per_peso)) *
            Decimal(str(multiplier))
        )

        # Cap by remaining budget
        budget_pts = int(budget_remaining * pts_per_peso)
        pts_earned = min(raw_points, budget_pts)

        if pts_earned <= 0:
            return self._empty_result(cliente_id, "ZERO_POINTS")

        pts_total = pts_before + pts_earned

        # Determine new level
        level_after = self._compute_level(cliente_id)
        level_up = (level_after != level_before and
                    LEVEL_ORDER.index(level_after) > LEVEL_ORDER.index(level_before))

        with self.db.transaction("LOYALTY_EARN"):
            # Update client points and level
            self.db.execute("""
                UPDATE clientes SET
                    puntos = ?,
                    nivel_fidelidad = ?
                WHERE id = ?
            """, (pts_total, level_after, cliente_id))

            # Log point transaction
            self.db.execute("""
                INSERT INTO historico_puntos (
                    cliente_id, tipo, puntos, saldo_anterior,
                    saldo_nuevo, descripcion, venta_id, created_at
                ) VALUES (?,?,?,?,?,?,?,?)
            """, (
                cliente_id, "GANADOS", pts_earned,
                pts_before, pts_total,
                f"Compra #{sale_id} ×{multiplier:.2f}",
                sale_id, self._now(),
            ))

            # Update budget spent
            self._deduct_budget(branch_id, pts_earned / pts_per_peso)

            # Layer 3: Update challenge progress
            challenges_updated = self._update_challenges(
                cliente_id, sale_total, sale_id, items, branch_id
            )

            # Layer 4: Update community goals
            community_updated = self._update_community_goals(
                sale_total, branch_id
            )

        # Build ticket messages
        messages = self._build_ticket_messages(
            cliente_id, pts_earned, pts_total, level_after,
            challenges_updated, community_updated
        )

        # Track ROI
        self._log_roi(branch_id, pts_earned, sale_total)

        return LoyaltyEarnResult(
            cliente_id=cliente_id,
            points_earned=pts_earned,
            points_total=pts_total,
            multiplier_used=multiplier,
            level_before=level_before,
            level_after=level_after,
            level_up=level_up,
            budget_remaining=budget_remaining,
            challenges_updated=challenges_updated,
            community_updated=community_updated,
            ticket_messages=messages,
            roi_logged=True,
        )

    # ── Layer 1: Points Redemption ────────────────────────────────────────────

    def redeem_points(self, cliente_id: int, points_to_redeem: int,
                      sale_total: float, branch_id: int) -> LoyaltyRedeemResult:
        limits = self._get_redemption_limits(branch_id)
        max_pct = limits["max_pct_per_sale"] / 100.0
        max_pts = limits["max_pts_per_sale"]

        # Check monthly redemption
        monthly_redeemed = self._monthly_redeemed(cliente_id)
        max_monthly = limits["max_monthly_pts"]
        remaining_monthly = max_monthly - monthly_redeemed
        if remaining_monthly <= 0:
            return LoyaltyRedeemResult(
                cliente_id=cliente_id,
                points_before=0, points_redeemed=0, points_after=0,
                discount_value=0, allowed=False,
                reason="MONTHLY_LIMIT_REACHED"
            )

        pts_to_redeem = min(points_to_redeem, max_pts, remaining_monthly)

        # Compute discount value — 1 punto = $1 pesos discount by default
        pts_value_rate = self._get_float_constant("LOYALTY_POINTS_VALUE_RATE", 1.0)
        discount_value = pts_to_redeem * pts_value_rate

        # Cap discount at max_pct of sale
        max_discount = sale_total * max_pct
        if discount_value > max_discount:
            discount_value = max_discount
            pts_to_redeem = int(discount_value / pts_value_rate)

        client_row = self.db.fetchone(
            "SELECT puntos FROM clientes WHERE id = ?", (cliente_id,)
        )
        if not client_row:
            return LoyaltyRedeemResult(
                cliente_id=cliente_id,
                points_before=0, points_redeemed=0, points_after=0,
                discount_value=0, allowed=False,
                reason="CLIENTE_NOT_FOUND"
            )

        pts_current = int(client_row["puntos"] or 0)
        if pts_to_redeem > pts_current:
            pts_to_redeem = pts_current
            discount_value = pts_to_redeem * pts_value_rate

        if pts_to_redeem <= 0:
            return LoyaltyRedeemResult(
                cliente_id=cliente_id,
                points_before=pts_current, points_redeemed=0,
                points_after=pts_current,
                discount_value=0, allowed=True,
                reason="NO_POINTS_TO_REDEEM"
            )

        pts_after = pts_current - pts_to_redeem

        with self.db.transaction("LOYALTY_REDEEM"):
            self.db.execute(
                "UPDATE clientes SET puntos = ? WHERE id = ?",
                (pts_after, cliente_id)
            )
            self.db.execute("""
                INSERT INTO historico_puntos (
                    cliente_id, tipo, puntos, saldo_anterior,
                    saldo_nuevo, descripcion, created_at
                ) VALUES (?,?,?,?,?,?,?)
            """, (
                cliente_id, "CANJEADOS", -pts_to_redeem,
                pts_current, pts_after,
                f"Canje descuento ${discount_value:.2f}",
                self._now(),
            ))

        return LoyaltyRedeemResult(
            cliente_id=cliente_id,
            points_before=pts_current,
            points_redeemed=pts_to_redeem,
            points_after=pts_after,
            discount_value=discount_value,
            allowed=True,
            reason="OK",
        )

    # ── Layer 2: Level Computation ────────────────────────────────────────────

    def _compute_level(self, cliente_id: int) -> str:
        from core.services.fidelidad_engine import FidelidadEngine
        try:
            row = self.db.fetchone("""
                SELECT score_total FROM loyalty_scores WHERE cliente_id = ?
            """, (cliente_id,))
            score = float(row["score_total"]) if row else 0.0
        except Exception:
            score = 0.0

        level = LEVEL_BRONZE
        for lvl in LEVEL_ORDER:
            if score >= LEVEL_THRESHOLDS[lvl]:
                level = lvl
        return level

    def get_level_progress(self, cliente_id: int) -> Dict:
        client_row = self.db.fetchone("""
            SELECT puntos, nivel_fidelidad FROM clientes WHERE id = ?
        """, (cliente_id,))
        if not client_row:
            return {}

        level = client_row["nivel_fidelidad"] or LEVEL_BRONZE
        idx = LEVEL_ORDER.index(level) if level in LEVEL_ORDER else 0
        pts = int(client_row["puntos"] or 0)

        if idx < len(LEVEL_ORDER) - 1:
            next_level = LEVEL_ORDER[idx + 1]
            pts_for_next = LEVEL_THRESHOLDS[next_level]
            pts_needed = max(0, pts_for_next - pts)
        else:
            next_level = level
            pts_needed = 0

        return {
            "level": level,
            "points": pts,
            "next_level": next_level,
            "pts_to_next_level": pts_needed,
        }

    # ── Layer 3: Challenges ───────────────────────────────────────────────────

    def _update_challenges(self, cliente_id: int, sale_total: float,
                            sale_id: int, items: List[Dict],
                            branch_id: int) -> List[int]:
        today = self._today()
        challenges = self.db.fetchall("""
            SELECT c.id, c.challenge_type, c.target_value, c.reward_points
            FROM loyalty_challenges c
            LEFT JOIN loyalty_challenge_progress p
                ON p.challenge_id = c.id AND p.cliente_id = ?
            WHERE c.is_active = 1
              AND c.start_date <= ?
              AND c.end_date >= ?
              AND (c.branch_id IS NULL OR c.branch_id = ?)
              AND (p.completed IS NULL OR p.completed = 0)
        """, (cliente_id, today, today, branch_id))

        updated = []
        for ch in challenges:
            ch_id   = ch["id"]
            ch_type = ch["challenge_type"]
            target  = float(ch["target_value"])
            reward  = int(ch["reward_points"])

            increment = 0.0
            if ch_type == "PURCHASE_COUNT":
                increment = 1.0
            elif ch_type == "AMOUNT":
                increment = sale_total
            elif ch_type == "PRODUCT":
                increment = sum(
                    float(i.get("cantidad", 0)) for i in items
                    if str(i.get("producto_id")) == str(target)
                )
            elif ch_type == "REFERRAL":
                increment = 0.0  # handled separately

            if increment <= 0:
                continue

            progress_row = self.db.fetchone("""
                SELECT id, current_value FROM loyalty_challenge_progress
                WHERE challenge_id = ? AND cliente_id = ?
            """, (ch_id, cliente_id))

            current_val = 0.0
            if progress_row:
                current_val = float(progress_row["current_value"] or 0)
                new_val = current_val + increment
                self.db.execute("""
                    UPDATE loyalty_challenge_progress
                    SET current_value = ?, completed = CASE WHEN ? >= ? THEN 1 ELSE 0 END,
                        completed_at = CASE WHEN ? >= ? THEN datetime('now') ELSE NULL END
                    WHERE id = ?
                """, (new_val, new_val, target, new_val, target, progress_row["id"]))
            else:
                new_val = increment
                self.db.execute("""
                    INSERT INTO loyalty_challenge_progress
                    (challenge_id, cliente_id, current_value, completed, completed_at)
                    VALUES (?,?,?,?,?)
                """, (ch_id, cliente_id, new_val,
                      1 if new_val >= target else 0,
                      self._now() if new_val >= target else None))

            # Grant reward if just completed
            if new_val >= target and current_val < target:
                self.db.execute("""
                    UPDATE loyalty_challenge_progress
                    SET reward_granted = 1
                    WHERE challenge_id = ? AND cliente_id = ?
                """, (ch_id, cliente_id))
                self.db.execute("""
                    UPDATE clientes SET puntos = puntos + ? WHERE id = ?
                """, (reward, cliente_id))
                self.db.execute("""
                    INSERT INTO historico_puntos (
                        cliente_id, tipo, puntos, saldo_anterior,
                        saldo_nuevo, descripcion, created_at
                    ) SELECT ?, 'RETO_COMPLETADO', ?, puntos - ?, puntos,
                              'Reto completado: ' || ?, ?
                    FROM clientes WHERE id = ?
                """, (cliente_id, reward, reward,
                      str(ch_id), self._now(), cliente_id))

            updated.append(ch_id)

        return updated

    # ── Layer 4: Community Goals ──────────────────────────────────────────────

    def _update_community_goals(self, sale_total: float,
                                  branch_id: int) -> List[int]:
        today = self._today()
        goals = self.db.fetchall("""
            SELECT id, target_value, current_value, reward_value
            FROM loyalty_community_goals
            WHERE is_active = 1
              AND start_date <= ?
              AND end_date >= ?
              AND completed = 0
              AND (branch_id IS NULL OR branch_id = ?)
        """, (today, today, branch_id))

        updated = []
        for goal in goals:
            goal_id  = goal["id"]
            new_val  = float(goal["current_value"] or 0) + sale_total
            target   = float(goal["target_value"])
            completed = new_val >= target

            self.db.execute("""
                UPDATE loyalty_community_goals
                SET current_value = ?,
                    completed = ?,
                    completed_at = ?
                WHERE id = ?
            """, (
                new_val,
                1 if completed else 0,
                self._now() if completed else None,
                goal_id,
            ))
            updated.append(goal_id)

        return updated

    def get_active_community_goal(self, branch_id: int) -> Optional[Dict]:
        today = self._today()
        row = self.db.fetchone("""
            SELECT id, name, target_value, current_value, reward_value, end_date
            FROM loyalty_community_goals
            WHERE is_active = 1
              AND start_date <= ?
              AND end_date >= ?
              AND completed = 0
              AND (branch_id IS NULL OR branch_id = ?)
            ORDER BY end_date ASC LIMIT 1
        """, (today, today, branch_id))
        return dict(row) if row else None

    # ── Ticket messages ───────────────────────────────────────────────────────

    def _build_ticket_messages(self, cliente_id: int, pts_earned: int,
                                pts_total: int, level: str,
                                challenges_updated: List[int],
                                community_updated: List[int]) -> List[str]:
        messages = []
        templates = self.db.fetchall("""
            SELECT message_type, message_template FROM loyalty_ticket_messages
            WHERE is_active = 1 ORDER BY priority DESC, id
        """)

        # Get progress data
        level_data = self.get_level_progress(cliente_id)
        community_goal = None
        if community_updated:
            row = self.db.fetchone("""
                SELECT name, target_value, current_value
                FROM loyalty_community_goals WHERE id = ?
            """, (community_updated[0],))
            community_goal = dict(row) if row else None

        challenge_data = None
        if challenges_updated:
            row = self.db.fetchone("""
                SELECT c.challenge_type, c.target_value,
                       COALESCE(p.current_value,0) AS progress
                FROM loyalty_challenges c
                LEFT JOIN loyalty_challenge_progress p
                    ON p.challenge_id = c.id AND p.cliente_id = ?
                WHERE c.id = ?
            """, (cliente_id, challenges_updated[0]))
            challenge_data = dict(row) if row else None

        for tmpl in templates:
            try:
                msg_type = tmpl["message_type"]
                template = tmpl["message_template"]
                ctx: Dict = {
                    "pts_earned": pts_earned,
                    "pts_total": pts_total,
                    "level": level,
                    "pts_next": level_data.get("pts_to_next_level", 0),
                    "next_level": level_data.get("next_level", level),
                }
                if msg_type == "COMMUNITY" and community_goal:
                    target = float(community_goal["target_value"]) or 1
                    ctx["community_pct"] = (
                        float(community_goal["current_value"]) / target * 100
                    )
                    ctx["community_name"] = community_goal["name"]
                elif msg_type == "CHALLENGE" and challenge_data:
                    target = float(challenge_data.get("target_value") or 1)
                    progress = float(challenge_data.get("progress") or 0)
                    ctx["challenge_name"] = challenge_data.get("challenge_type", "")
                    ctx["progress"] = min(progress / target * 100, 100)
                elif msg_type == "GENERIC":
                    pass
                else:
                    continue

                messages.append(template.format(**ctx))
            except (KeyError, ValueError, ZeroDivisionError):
                continue

        return messages

    def get_ticket_engagement(self, cliente_id: int,
                               branch_id: int) -> TicketEngagementData:
        client_row = self.db.fetchone("""
            SELECT puntos, nivel_fidelidad FROM clientes WHERE id = ?
        """, (cliente_id,))
        pts_total = int(client_row["puntos"] or 0) if client_row else 0
        level     = (client_row["nivel_fidelidad"] if client_row else None) or LEVEL_BRONZE

        level_data = self.get_level_progress(cliente_id)
        community  = self.get_active_community_goal(branch_id)

        challenge_row = self.db.fetchone("""
            SELECT c.challenge_type,
                   COALESCE(p.current_value,0) AS prog,
                   c.target_value
            FROM loyalty_challenges c
            LEFT JOIN loyalty_challenge_progress p
                ON p.challenge_id = c.id AND p.cliente_id = ?
            WHERE c.is_active = 1
              AND c.end_date >= DATE('now')
              AND (c.branch_id IS NULL OR c.branch_id = ?)
              AND (p.completed IS NULL OR p.completed = 0)
            ORDER BY c.end_date ASC LIMIT 1
        """, (cliente_id, branch_id))

        community_pct = 0.0
        if community:
            t = float(community["target_value"]) or 1
            community_pct = min(float(community["current_value"]) / t * 100, 100)

        challenge_name     = None
        challenge_progress = 0.0
        if challenge_row:
            t2 = float(challenge_row["target_value"]) or 1
            challenge_name     = challenge_row["challenge_type"]
            challenge_progress = min(float(challenge_row["prog"]) / t2 * 100, 100)

        messages = self._build_ticket_messages(
            cliente_id, 0, pts_total, level, [], []
        )

        return TicketEngagementData(
            points_earned=0,
            points_total=pts_total,
            level=level,
            pts_to_next_level=level_data.get("pts_to_next_level", 0),
            next_level=level_data.get("next_level", level),
            community_goal_name=community["name"] if community else None,
            community_pct=community_pct,
            challenge_name=challenge_name,
            challenge_progress=challenge_progress,
            messages=messages,
        )

    # ── Budget cap helpers ────────────────────────────────────────────────────

    def _check_budget_cap(self, branch_id: int, sale_total: float,
                           pts_per_peso: float) -> float:
        ym = self._year_month()
        row = self.db.fetchone("""
            SELECT budget_limit, spent_amount FROM loyalty_budget_caps
            WHERE branch_id = ? AND year_month = ?
        """, (branch_id, ym))
        if not row:
            return sale_total  # No cap configured — unlimited
        remaining = float(row["budget_limit"]) - float(row["spent_amount"] or 0)
        return max(0.0, remaining)

    def _deduct_budget(self, branch_id: int, amount: float) -> None:
        ym = self._year_month()
        try:
            self.db.execute("""
                INSERT INTO loyalty_budget_caps (branch_id, year_month, budget_limit, spent_amount)
                VALUES (?,?,0,?)
                ON CONFLICT(branch_id, year_month)
                DO UPDATE SET spent_amount = spent_amount + excluded.spent_amount
            """, (branch_id, ym, amount))
        except Exception as exc:
            logger.warning("budget_deduct failed: %s", exc)

    def _monthly_redeemed(self, cliente_id: int) -> int:
        ym = self._year_month()
        row = self.db.fetchone("""
            SELECT COALESCE(SUM(ABS(puntos)),0) AS total
            FROM historico_puntos
            WHERE cliente_id = ?
              AND tipo = 'CANJEADOS'
              AND strftime('%Y-%m', created_at) = ?
        """, (cliente_id, ym))
        return int(row["total"]) if row else 0

    # ── ROI tracking ──────────────────────────────────────────────────────────

    def _log_roi(self, branch_id: int, pts_earned: int,
                  sale_total: float) -> None:
        ym = self._year_month()
        pts_value = pts_earned * self._get_float_constant(
            "LOYALTY_POINTS_VALUE_RATE", 1.0
        )
        try:
            self.db.execute("""
                INSERT INTO loyalty_roi_tracking (
                    branch_id, year_month, points_issued,
                    cost_of_rewards, revenue_from_loyal_customers
                ) VALUES (?,?,?,?,?)
                ON CONFLICT(branch_id, year_month)
                DO UPDATE SET
                    points_issued = points_issued + excluded.points_issued,
                    cost_of_rewards = cost_of_rewards + excluded.cost_of_rewards,
                    revenue_from_loyal_customers =
                        revenue_from_loyal_customers + excluded.revenue_from_loyal_customers
            """, (branch_id, ym, pts_earned, pts_value, sale_total))
        except Exception as exc:
            logger.warning("roi_log failed: %s", exc)

    def compute_roi(self, branch_id: int, year_month: str) -> Optional[Dict]:
        row = self.db.fetchone("""
            SELECT * FROM loyalty_roi_tracking
            WHERE branch_id = ? AND year_month = ?
        """, (branch_id, year_month))
        if not row:
            return None
        result = dict(row)
        revenue = float(result.get("revenue_from_loyal_customers") or 1)
        cost    = float(result.get("cost_of_rewards") or 0)
        result["roi_pct"] = round((revenue - cost) / revenue * 100, 2) if revenue else 0
        self.db.execute("""
            UPDATE loyalty_roi_tracking SET roi_pct = ?, computed_at = ?
            WHERE branch_id = ? AND year_month = ?
        """, (result["roi_pct"], self._now(), branch_id, year_month))
        return result

    # ── Multiplier computation ────────────────────────────────────────────────

    def _compute_multiplier(self, cliente_id: int, level: str,
                             branch_id: int,
                             items: List[Dict]) -> float:
        rules = self.db.fetchall("""
            SELECT rule_type, condition_value, multiplier
            FROM loyalty_multiplier_rules
            WHERE is_active = 1
            ORDER BY priority DESC, id
        """)

        best = 1.0
        now = datetime.utcnow()

        for rule in rules:
            rtype = rule["rule_type"]
            cond  = rule["condition_value"]
            mult  = float(rule["multiplier"])

            if rtype == "LEVEL" and cond == level:
                best = max(best, mult)
            elif rtype == "DAY_OF_WEEK":
                try:
                    if now.weekday() == int(cond):
                        best = max(best, mult)
                except ValueError:
                    pass
            elif rtype == "HOUR_RANGE":
                try:
                    start_h, end_h = map(int, cond.split("-"))
                    if start_h <= now.hour < end_h:
                        best = max(best, mult)
                except ValueError:
                    pass
            elif rtype == "PRODUCT_CATEGORY":
                # Check if any item belongs to this category
                for item in items:
                    p_row = self.db.fetchone(
                        "SELECT categoria FROM productos WHERE id = ?",
                        (item.get("producto_id"),)
                    )
                    if p_row and p_row["categoria"] == cond:
                        best = max(best, mult)
                        break

        return best

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _empty_result(self, cliente_id: int, reason: str) -> LoyaltyEarnResult:
        client_row = self.db.fetchone(
            "SELECT puntos, nivel_fidelidad FROM clientes WHERE id = ?",
            (cliente_id,)
        )
        pts_total = int(client_row["puntos"] or 0) if client_row else 0
        level     = (client_row["nivel_fidelidad"] if client_row else None) or LEVEL_BRONZE
        return LoyaltyEarnResult(
            cliente_id=cliente_id,
            points_earned=0,
            points_total=pts_total,
            multiplier_used=1.0,
            level_before=level,
            level_after=level,
            level_up=False,
            budget_remaining=0,
            challenges_updated=[],
            community_updated=[],
            ticket_messages=[],
            roi_logged=False,
        )
