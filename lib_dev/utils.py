"""
Utility enums for the Smartbetting project.

This module contains enumeration classes for defining constants used
throughout the Smartbetting data pipeline.
"""

from enum import Enum


class Bucket(Enum):
    """
    Data bucket enumeration.

    Defines the available data buckets for organizing data.
    """

    LAMJAV_STORAGE = "lamjav_storage"
    SMARTBETTING_STORAGE = "smartbetting-landing"

    def __str__(self) -> str:
        """
        Return the string representation of the bucket value.

        Returns:
            String value of the bucket
        """
        return "%s" % self.value


class Catalog(Enum):
    """
    Data catalog enumeration.

    Defines the available data catalogs for organizing data.
    """

    NBA = "nba"
    ODDS = "odds"
    INJURY_REPORT = "injury_report"

    def __str__(self) -> str:
        """
        Return the string representation of the catalog value.

        Returns:
            String value of the catalog
        """
        return "%s" % self.value


class Schema(Enum):
    """
    Data schema enumeration.

    Defines the available data schemas for data quality layers.
    """

    LANDING = "landing"
    RAW = "raw"

    def __str__(self) -> str:
        """
        Return the string representation of the schema value.

        Returns:
            String value of the schema
        """
        return "%s" % self.value


class Table(Enum):
    """
    Data table enumeration.

    Defines the available data tables for specific entities.
    """

    TEAMS = "teams"
    PLAYERS = "players"
    GAMES = "games"
    GAME_PLAYER_STATS = "game_player_stats"
    ACTIVE_PLAYERS = "active_players"
    PLAYER_INJURIES = "player_injuries"
    SEASON_AVERAGES = "season_averages"
    ADVANCED_STATS = "advanced_stats"
    LEADERS = "leaders"
    SPORTS = "sports"
    ODDS = "odds"
    PARTICIPANTS = "participants"
    HISTORICAL_ODDS = "historical_odds"
    HISTORICAL_EVENTS = "historical_events"
    INJURY_REPORT = "injury_report"
    TEAM_STANDINGS = "team_standings"

    def __str__(self) -> str:
        """
        Return the string representation of the table value.

        Returns:
            String value of the table
        """
        return "%s" % self.value


class Season(Enum):
    """
    Season enumeration.

    Defines the available seasons for data processing.
    """

    SEASON_2024 = 2024
    SEASON_2025 = 2025
    SEASON_2026 = 2026
    SEASON_2027 = 2027

    def __str__(self) -> str:
        """
        Return the string representation of the table value.

        Returns:
            String value of the table
        """
        return "%s" % self.value
