"""
Grandpa Joe Personality - The wise old handicapper.
Generates folksy race commentary and betting wisdom.
"""

import random
from typing import Dict, List, Optional


class GrandpaJoePersonality:
    """
    Grandpa Joe: wise, folksy, honest about the track.
    Knows racing inside and out. Cares about the people he helps.
    Not a shill — tells you when a race is unplayable.
    """

    GREETINGS = [
        "Beautiful day at the track, kid. Let's find some winners.",
        "Pull up a chair, son. Grandpa Joe's been studying the form.",
        "Morning! Coffee's hot and the Racing Form's got ink on it. Let's go.",
        "Alright kid, what track we looking at today?",
        "The horses are warming up and so am I. What do you need?",
    ]

    WIN_CELEBRATIONS = [
        "Now THAT'S what I call a good eye! We cashed that ticket!",
        "Winner winner! That's the kind of pick that keeps us coming back.",
        "Told ya, kid. When the old man likes a horse, you listen.",
        "That's racing at its finest. Collect your winnings and smile.",
        "See? Patience and homework. That's how you beat the track.",
    ]

    LOSS_CONSOLATIONS = [
        "That's racing, kid. The best horse doesn't always win.",
        "Can't win 'em all. The key is winning more than you lose.",
        "Tough beat. But we'll get 'em next race.",
        "Sometimes the track has other plans. Stay disciplined.",
        "Bad beat, but our process was sound. That's what matters long term.",
    ]

    NO_PLAY_WARNINGS = [
        "I don't see a play here, kid. Sometimes the best bet is no bet.",
        "This race is a coin flip. Save your money for one where we have an edge.",
        "Grandpa Joe says pass. No edge, no bet. That's the discipline.",
        "I wouldn't touch this race with a ten-foot pole. Too many unknowns.",
        "The smart money sits this one out. Next race might be better.",
    ]

    def greeting(self, stats: Optional[Dict] = None) -> str:
        """Generate a morning greeting."""
        msg = random.choice(self.GREETINGS)
        if stats:
            horses = stats.get("horses", 0)
            races = stats.get("races", 0)
            if horses > 0:
                msg += f"\n  Got {horses:,} horses and {races:,} races in the brain."
            pnl = stats.get("net_pnl", 0)
            if pnl > 0:
                msg += f"\n  We're up ${pnl:,.2f}. Let's keep it going."
            elif pnl < 0:
                msg += f"\n  We're down ${abs(pnl):,.2f}. Time to be selective."
        return msg

    def narrate_picks(self, rankings: List[Dict], race_info: Dict) -> str:
        """Generate folksy analysis of handicapping picks."""
        if not rankings:
            return random.choice(self.NO_PLAY_WARNINGS)

        track = race_info.get("track_code", "the track")
        race_num = race_info.get("race_number", "")
        surface = race_info.get("surface", "dirt")
        distance = race_info.get("distance_furlongs", 6)

        lines = [f"Alright, Race {race_num} at {track}. "
                 f"{distance}f on the {surface}. Here's what I see:\n"]

        top = rankings[0]
        win_pct = top.get("win_probability", 0) * 100

        # Top pick narrative
        if win_pct > 35:
            lines.append(f"  #{top['post_position']} {top['horse_name']} — "
                        f"This one stands out. {win_pct:.0f}% chance. "
                        "I like this horse a lot today.")
        elif win_pct > 20:
            lines.append(f"  #{top['post_position']} {top['horse_name']} — "
                        f"Solid pick at {win_pct:.0f}%. Not a lock, but the best of this bunch.")
        else:
            lines.append(f"  #{top['post_position']} {top['horse_name']} — "
                        f"Top of a wide-open field at {win_pct:.0f}%. Tread carefully.")

        # Second pick
        if len(rankings) > 1:
            second = rankings[1]
            lines.append(f"  #{second['post_position']} {second['horse_name']} — "
                        f"The main threat. Watch for this one late.")

        # Third pick
        if len(rankings) > 2:
            third = rankings[2]
            lines.append(f"  #{third['post_position']} {third['horse_name']} — "
                        f"A price play if things set up right.")

        # Overall confidence
        top_confidence = top.get("confidence", 0)
        if top_confidence < 0.3:
            lines.append(f"\n  ⚠️  Low confidence race. {random.choice(self.NO_PLAY_WARNINGS)}")
        elif top_confidence > 0.6:
            lines.append("\n  Feeling good about this one, kid.")

        return "\n".join(lines)

    def comment_on_bet(self, bet_type: str, amount: float,
                       horse_name: str, odds: float) -> str:
        """Commentary on a suggested or placed bet."""
        if amount > 20:
            return (f"${amount:.0f} to {bet_type} on {horse_name} at "
                    f"{odds-1:.0f}-1. That's a real bet. Make sure you're comfortable.")
        else:
            return (f"${amount:.0f} to {bet_type} on {horse_name}. "
                    f"Smart money, measured bet. I like the discipline.")

    def win_celebration(self, payout: float) -> str:
        """Celebrate a winning bet."""
        msg = random.choice(self.WIN_CELEBRATIONS)
        if payout > 100:
            msg += f" ${payout:,.2f} in the pocket!"
        elif payout > 20:
            msg += f" ${payout:,.2f}. Not bad at all."
        return msg

    def loss_consolation(self) -> str:
        """Console after a loss."""
        return random.choice(self.LOSS_CONSOLATIONS)

    def session_summary(self, stats: Dict) -> str:
        """End-of-session summary."""
        wagered = stats.get("total_wagered", 0)
        returned = stats.get("total_returned", 0)
        bets = stats.get("total_bets", 0)
        net = returned - wagered
        roi = stats.get("roi", 0)

        lines = ["Here's how we did today:\n"]
        lines.append(f"  Bets placed: {bets}")
        lines.append(f"  Total wagered: ${wagered:,.2f}")
        lines.append(f"  Total returned: ${returned:,.2f}")

        if net >= 0:
            lines.append(f"  Net profit: [green]${net:,.2f}[/green] ({roi:+.1f}% ROI)")
            lines.append("\n  Good day at the track, kid. See you tomorrow.")
        else:
            lines.append(f"  Net loss: [red]-${abs(net):,.2f}[/red] ({roi:+.1f}% ROI)")
            lines.append("\n  Tough day. But we'll be back. The track ain't going anywhere.")

        return "\n".join(lines)
