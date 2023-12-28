# standard library imports

# local imports

# third party imports
from scrapy import Field, Item


class FightMatrixRankingItem(Item):
    """
    Item class for ranking data from FightMatrix
    """

    FIGHTER_ID = Field()
    DATE = Field()
    WEIGHT_CLASS = Field()
    RANK = Field()
    RANK_CHANGE = Field()
    POINTS = Field()


class FightMatrixFighterItem(Item):
    """
    Item class for fighter data from FightMatrix
    """

    FIGHTER_NAME = Field()
    FIGHTER_ID = Field()
    TAPOLOGY_FIGHTER_ID = Field()
    SHERDOG_FIGHTER_ID = Field()
