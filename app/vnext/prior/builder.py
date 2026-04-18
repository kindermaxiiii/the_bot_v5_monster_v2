from __future__ import annotations

from app.vnext.data.normalized_models import HistoricalDataset, worst_quality_flag
from app.vnext.prior.models import (
    AttackContext,
    CompetitionContext,
    CompetitionSnapshot,
    DefenseContext,
    FormContext,
    HistoricalPriorPack,
    MatchupContext,
    MatchupSnapshot,
    StrengthContext,
    StyleContext,
    TeamAttackSnapshot,
    TeamDefenseSnapshot,
    TeamFormSnapshot,
    TeamStrengthSnapshot,
    TeamStyleSnapshot,
    TeamVenueSnapshot,
    VenueContext,
)
from app.vnext.profiles.builders import (
    build_competition_profile,
    build_matchup_profile,
    build_team_recent_profile,
    build_team_strength_profile,
    build_team_style_profile,
    build_team_venue_profile,
)


def _pair_block_meta(home_profile, away_profile, *, source: str) -> dict[str, object]:
    return {
        "source": source,
        "sample_size": min(home_profile.sample_size, away_profile.sample_size),
        "confidence_weight": round(
            min(home_profile.confidence_weight, away_profile.confidence_weight), 4
        ),
        "data_quality_flag": worst_quality_flag(
            home_profile.data_quality_flag,
            away_profile.data_quality_flag,
        ),
    }


def build_historical_prior_pack(
    dataset: HistoricalDataset,
    *,
    fixture_id: int,
) -> HistoricalPriorPack:
    fixture = dataset.fixture_by_id(fixture_id)
    as_of = fixture.kickoff_utc

    home_recent = build_team_recent_profile(
        dataset,
        team_id=fixture.home_team_id,
        as_of=as_of,
        competition_id=fixture.competition_id,
    )
    away_recent = build_team_recent_profile(
        dataset,
        team_id=fixture.away_team_id,
        as_of=as_of,
        competition_id=fixture.competition_id,
    )
    home_venue = build_team_venue_profile(
        dataset,
        team_id=fixture.home_team_id,
        venue="HOME",
        as_of=as_of,
        competition_id=fixture.competition_id,
    )
    away_venue = build_team_venue_profile(
        dataset,
        team_id=fixture.away_team_id,
        venue="AWAY",
        as_of=as_of,
        competition_id=fixture.competition_id,
    )
    home_strength = build_team_strength_profile(
        dataset,
        team_id=fixture.home_team_id,
        as_of=as_of,
        competition_id=fixture.competition_id,
    )
    away_strength = build_team_strength_profile(
        dataset,
        team_id=fixture.away_team_id,
        as_of=as_of,
        competition_id=fixture.competition_id,
    )
    home_style = build_team_style_profile(
        dataset,
        team_id=fixture.home_team_id,
        as_of=as_of,
        competition_id=fixture.competition_id,
    )
    away_style = build_team_style_profile(
        dataset,
        team_id=fixture.away_team_id,
        as_of=as_of,
        competition_id=fixture.competition_id,
    )
    matchup = build_matchup_profile(
        dataset,
        home_team_id=fixture.home_team_id,
        away_team_id=fixture.away_team_id,
        as_of=as_of,
    )
    competition = build_competition_profile(
        dataset,
        competition_id=fixture.competition_id,
        season=fixture.season,
        as_of=as_of,
    )

    attack_context = AttackContext(
        **_pair_block_meta(home_recent, away_recent, source="team_recent_profile.v1"),
        home=TeamAttackSnapshot(
            team_id=home_recent.team_id,
            team_name=home_recent.team_name,
            goals_for_per_match=home_recent.goals_for_per_match,
            xg_for_per_match=home_recent.xg_for_per_match,
            shots_for_per_match=home_recent.shots_for_per_match,
            shots_on_for_per_match=home_recent.shots_on_for_per_match,
        ),
        away=TeamAttackSnapshot(
            team_id=away_recent.team_id,
            team_name=away_recent.team_name,
            goals_for_per_match=away_recent.goals_for_per_match,
            xg_for_per_match=away_recent.xg_for_per_match,
            shots_for_per_match=away_recent.shots_for_per_match,
            shots_on_for_per_match=away_recent.shots_on_for_per_match,
        ),
    )
    defense_context = DefenseContext(
        **_pair_block_meta(home_recent, away_recent, source="team_recent_profile.v1"),
        home=TeamDefenseSnapshot(
            team_id=home_recent.team_id,
            team_name=home_recent.team_name,
            goals_against_per_match=home_recent.goals_against_per_match,
            xg_against_per_match=home_recent.xg_against_per_match,
            shots_on_against_per_match=home_recent.shots_on_against_per_match,
            clean_sheet_rate=home_recent.clean_sheet_rate,
        ),
        away=TeamDefenseSnapshot(
            team_id=away_recent.team_id,
            team_name=away_recent.team_name,
            goals_against_per_match=away_recent.goals_against_per_match,
            xg_against_per_match=away_recent.xg_against_per_match,
            shots_on_against_per_match=away_recent.shots_on_against_per_match,
            clean_sheet_rate=away_recent.clean_sheet_rate,
        ),
    )
    venue_context = VenueContext(
        **_pair_block_meta(home_venue, away_venue, source="team_venue_profile.v1"),
        home=TeamVenueSnapshot(
            team_id=home_venue.team_id,
            team_name=home_venue.team_name,
            venue=home_venue.venue,
            goals_for_per_match=home_venue.goals_for_per_match,
            goals_against_per_match=home_venue.goals_against_per_match,
            xg_for_per_match=home_venue.xg_for_per_match,
            xg_against_per_match=home_venue.xg_against_per_match,
            shrinkage_weight=home_venue.shrinkage_weight,
        ),
        away=TeamVenueSnapshot(
            team_id=away_venue.team_id,
            team_name=away_venue.team_name,
            venue=away_venue.venue,
            goals_for_per_match=away_venue.goals_for_per_match,
            goals_against_per_match=away_venue.goals_against_per_match,
            xg_for_per_match=away_venue.xg_for_per_match,
            xg_against_per_match=away_venue.xg_against_per_match,
            shrinkage_weight=away_venue.shrinkage_weight,
        ),
    )
    form_context = FormContext(
        **_pair_block_meta(home_recent, away_recent, source="team_recent_profile.v1"),
        home=TeamFormSnapshot(
            team_id=home_recent.team_id,
            team_name=home_recent.team_name,
            points_per_match=home_recent.points_per_match,
            form_score=home_recent.form_score,
            clean_sheet_rate=home_recent.clean_sheet_rate,
            failed_to_score_rate=home_recent.failed_to_score_rate,
        ),
        away=TeamFormSnapshot(
            team_id=away_recent.team_id,
            team_name=away_recent.team_name,
            points_per_match=away_recent.points_per_match,
            form_score=away_recent.form_score,
            clean_sheet_rate=away_recent.clean_sheet_rate,
            failed_to_score_rate=away_recent.failed_to_score_rate,
        ),
    )
    strength_context = StrengthContext(
        **_pair_block_meta(home_strength, away_strength, source="team_strength_profile.v1"),
        home=TeamStrengthSnapshot(
            team_id=home_strength.team_id,
            team_name=home_strength.team_name,
            global_rating=home_strength.global_rating,
            offensive_rating=home_strength.offensive_rating,
            defensive_rating=home_strength.defensive_rating,
            stability_score=home_strength.stability_score,
        ),
        away=TeamStrengthSnapshot(
            team_id=away_strength.team_id,
            team_name=away_strength.team_name,
            global_rating=away_strength.global_rating,
            offensive_rating=away_strength.offensive_rating,
            defensive_rating=away_strength.defensive_rating,
            stability_score=away_strength.stability_score,
        ),
    )
    style_context = StyleContext(
        **_pair_block_meta(home_style, away_style, source="team_style_profile.v1"),
        home=TeamStyleSnapshot(
            team_id=home_style.team_id,
            team_name=home_style.team_name,
            btts_rate=home_style.btts_rate,
            under_2_5_rate=home_style.under_2_5_rate,
            over_2_5_rate=home_style.over_2_5_rate,
            team_total_over_0_5_rate=home_style.team_total_over_0_5_rate,
            team_total_over_1_5_rate=home_style.team_total_over_1_5_rate,
            team_total_over_2_5_rate=home_style.team_total_over_2_5_rate,
            clean_sheet_rate=home_style.clean_sheet_rate,
            failed_to_score_rate=home_style.failed_to_score_rate,
        ),
        away=TeamStyleSnapshot(
            team_id=away_style.team_id,
            team_name=away_style.team_name,
            btts_rate=away_style.btts_rate,
            under_2_5_rate=away_style.under_2_5_rate,
            over_2_5_rate=away_style.over_2_5_rate,
            team_total_over_0_5_rate=away_style.team_total_over_0_5_rate,
            team_total_over_1_5_rate=away_style.team_total_over_1_5_rate,
            team_total_over_2_5_rate=away_style.team_total_over_2_5_rate,
            clean_sheet_rate=away_style.clean_sheet_rate,
            failed_to_score_rate=away_style.failed_to_score_rate,
        ),
    )
    matchup_context = MatchupContext(
        source=matchup.source,
        sample_size=matchup.sample_size,
        confidence_weight=matchup.confidence_weight,
        data_quality_flag=matchup.data_quality_flag,
        matchup=MatchupSnapshot(
            home_team_goals_per_match=matchup.home_team_goals_per_match,
            away_team_goals_per_match=matchup.away_team_goals_per_match,
            btts_rate=matchup.btts_rate,
            over_2_5_rate=matchup.over_2_5_rate,
            draw_rate=matchup.draw_rate,
            seasons_covered=matchup.seasons_covered,
        ),
    )
    competition_context = CompetitionContext(
        source=competition.source,
        sample_size=competition.sample_size,
        confidence_weight=competition.confidence_weight,
        data_quality_flag=competition.data_quality_flag,
        competition=CompetitionSnapshot(
            competition_id=competition.competition_id,
            competition_name=competition.competition_name,
            season=competition.season,
            avg_goals_per_match=competition.avg_goals_per_match,
            btts_rate=competition.btts_rate,
            over_2_5_rate=competition.over_2_5_rate,
            data_quality_score=competition.data_quality_score,
            market_depth_score=competition.market_depth_score,
            competition_confidence_score=competition.competition_confidence_score,
            variance_score=competition.variance_score,
        ),
    )
    return HistoricalPriorPack(
        fixture_id=fixture.fixture_id,
        competition_id=fixture.competition_id,
        season=fixture.season,
        as_of_date=fixture.kickoff_utc.date(),
        kickoff_utc=fixture.kickoff_utc,
        home_team_id=fixture.home_team_id,
        away_team_id=fixture.away_team_id,
        home_team_name=fixture.home_team_name,
        away_team_name=fixture.away_team_name,
        source_version="vnext_sprint1",
        attack_context=attack_context,
        defense_context=defense_context,
        venue_context=venue_context,
        form_context=form_context,
        strength_context=strength_context,
        style_context=style_context,
        matchup_context=matchup_context,
        competition_context=competition_context,
    )
