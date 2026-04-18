from __future__ import annotations

from app.vnext.markets.models import LineTemplate


LINE_TEMPLATES: dict[str, LineTemplate] = {
    "OU_FT_OVER_CORE": LineTemplate(
        key="OU_FT_OVER_CORE",
        family="OU_FT",
        direction="OVER",
        suggested_line_family="over_1_5_or_2_5",
        label="OU FT Over Core",
    ),
    "OU_FT_UNDER_CORE": LineTemplate(
        key="OU_FT_UNDER_CORE",
        family="OU_FT",
        direction="UNDER",
        suggested_line_family="under_2_5_or_3_5",
        label="OU FT Under Core",
    ),
    "BTTS_YES_CORE": LineTemplate(
        key="BTTS_YES_CORE",
        family="BTTS",
        direction="YES",
        suggested_line_family="btts_yes_core",
        label="BTTS Yes Core",
    ),
    "BTTS_NO_CORE": LineTemplate(
        key="BTTS_NO_CORE",
        family="BTTS",
        direction="NO",
        suggested_line_family="btts_no_core",
        label="BTTS No Core",
    ),
    "TEAM_TOTAL_HOME_OVER_CORE": LineTemplate(
        key="TEAM_TOTAL_HOME_OVER_CORE",
        family="TEAM_TOTAL",
        direction="HOME_OVER",
        suggested_line_family="home_over_0_5_or_1_5",
        label="Team Total Home Over Core",
    ),
    "TEAM_TOTAL_AWAY_OVER_CORE": LineTemplate(
        key="TEAM_TOTAL_AWAY_OVER_CORE",
        family="TEAM_TOTAL",
        direction="AWAY_OVER",
        suggested_line_family="away_over_0_5_or_1_5",
        label="Team Total Away Over Core",
    ),
    "TEAM_TOTAL_HOME_UNDER_CORE": LineTemplate(
        key="TEAM_TOTAL_HOME_UNDER_CORE",
        family="TEAM_TOTAL",
        direction="HOME_UNDER",
        suggested_line_family="home_under_1_5_or_2_5",
        label="Team Total Home Under Core",
    ),
    "TEAM_TOTAL_AWAY_UNDER_CORE": LineTemplate(
        key="TEAM_TOTAL_AWAY_UNDER_CORE",
        family="TEAM_TOTAL",
        direction="AWAY_UNDER",
        suggested_line_family="away_under_1_5_or_2_5",
        label="Team Total Away Under Core",
    ),
    "RESULT_HOME_CORE": LineTemplate(
        key="RESULT_HOME_CORE",
        family="RESULT",
        direction="HOME",
        suggested_line_family="home_result_lab_only",
        label="Result Home Core",
    ),
    "RESULT_AWAY_CORE": LineTemplate(
        key="RESULT_AWAY_CORE",
        family="RESULT",
        direction="AWAY",
        suggested_line_family="away_result_lab_only",
        label="Result Away Core",
    ),
}


def line_template(template_key: str) -> LineTemplate:
    return LINE_TEMPLATES[template_key]
