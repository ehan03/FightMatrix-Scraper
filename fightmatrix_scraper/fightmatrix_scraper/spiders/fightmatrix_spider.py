# standard library imports
import time

# third party imports
import pandas as pd
from scrapy.spiders import Spider

# local imports
from ..items import FightMatrixEloItem, FightMatrixFighterItem, FightMatrixRankingItem


class FightMatrixSpider(Spider):
    """
    Spider for scraping data from FightMatrix
    """

    name = "fightmatrix_spider"
    allowed_domains = ["fightmatrix.com"]
    start_urls = [
        "https://www.fightmatrix.com/historical-mma-rankings/ranking-snapshots/"
    ]
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        "CONCURRENT_REQUESTS": 10,
        "COOKIES_ENABLED": False,
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "scrapy_user_agents.middlewares.RandomUserAgentMiddleware": 400,
        },
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "FEED_EXPORT_ENCODING": "utf-8",
        "DEPTH_PRIORITY": 1,
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
        "RETRY_TIMES": 5,
        "LOG_LEVEL": "INFO",
        "ITEM_PIPELINES": {
            #
        },
        "CLOSESPIDER_ERRORCOUNT": 1,
    }

    def __init__(self, *args, scrape_type, **kwargs):
        """
        Initialize FightMatrixSpider
        """

        super().__init__(*args, **kwargs)
        assert scrape_type in {"most_recent", "all"}
        self.scrape_type = scrape_type
        self.weight_class_map = {
            "1": "Heavyweight",
            "2": "Light Heavyweight",
            "3": "Middleweight",
            "4": "Welterweight",
            "5": "Lightweight",
            "6": "Featherweight",
            "7": "Bantamweight",
            "8": "Flyweight",
            "16": "Women's Featherweight",
            "15": "Women's Bantamweight",
            "14": "Women's Flyweight",
            "13": "Women's Strawweight",
        }

    def parse(self, response):
        filtertable_td = response.css("table#filterTable *> td")
        issues = filtertable_td[0].css("option::attr(value)").getall()[1:]
        dates = filtertable_td[0].css("option::text").getall()[1:]
        dates = [pd.to_datetime(x).strftime("%Y-%m-%d") for x in dates]
        assert len(issues) == len(dates)

        if self.scrape_type == "most_recent":
            issues = [issues[0]]
            dates = [dates[0]]

        for issue, date in zip(issues, dates):
            for division, weight_class in self.weight_class_map.items():
                if time.strptime(date, "%Y-%m-%d") < time.strptime(
                    "2010-03-01", "%Y-%m-%d"
                ):
                    # UFCStats data is useless before 2010 (no red/blue distinction)
                    break

                if time.strptime(date, "%Y-%m-%d") < time.strptime(
                    "2013-02-01", "%Y-%m-%d"
                ) and division in {"16", "15", "14", "13"}:
                    # Women's divisions didn't exist before 2013 in the UFC
                    continue

                yield response.follow(
                    f"https://www.fightmatrix.com/historical-mma-rankings/ranking-snapshots/?Issue={issue}&Division={division}",
                    callback=self.parse_ranking_page,
                    cb_kwargs={"date": date, "weight_class": weight_class},
                )

    def parse_ranking_page(self, response, date, weight_class):
        rows = response.css("table.tblRank > tbody > tr")
        for row in rows[1:]:
            ranking_item = FightMatrixRankingItem()
            ranking_item["DATE"] = date
            ranking_item["WEIGHT_CLASS"] = weight_class

            cells = row.css("td")

            ranking_item["RANK"] = int(cells[0].css("::text").get().strip())
            rank_change = cells[1].css("::text").get().strip()
            if rank_change == "NR":
                ranking_item["RANK_CHANGE"] = None
            elif not rank_change:
                ranking_item["RANK_CHANGE"] = 0
            else:
                ranking_item["RANK_CHANGE"] = int(rank_change)

            fighter_link = cells[2].css("a::attr(href)").get()
            fighter_id = fighter_link.replace("/fighter-profile/", "")

            if fighter_id == "//":
                # Edge case for missing fighter
                continue

            ranking_item["FIGHTER_ID"] = fighter_id
            ranking_item["POINTS"] = int(cells[3].css("div.tdBar::text").get().strip())

            yield ranking_item

            yield response.follow(
                fighter_link,
                callback=self.parse_fighter,
            )

        pager_table = response.css("table.pager")[0]
        pager_atags = pager_table.css("tr > td > a")
        if pager_atags:
            for atag in pager_atags:
                arrow = atag.css("b::text").get().strip()
                href = atag.css("::attr(href)").get()
                if arrow == ">":
                    yield response.follow(
                        href,
                        callback=self.parse_ranking_page,
                        cb_kwargs={"date": date, "weight_class": weight_class},
                    )
                    break

    def parse_fighter(self, response):
        fighter_item = FightMatrixFighterItem()

        fighter_item["FIGHTER_NAME"] = (
            response.css("div.posttitle > h1 > a::text").get().strip()
        )
        fighter_item["FIGHTER_ID"] = response.url.replace(
            "https://www.fightmatrix.com/fighter-profile/", ""
        )
        fighter_links = response.css(
            "td.tdRankHead > div.leftCol *> a::attr(href)"
        ).getall()
        sherdog_fighter_id = tapology_fighter_id = None
        for link in fighter_links:
            if "www.sherdog.com" in link:
                sherdog_fighter_id = link.split("/")[-1]
            elif "www.tapology.com" in link:
                tapology_fighter_id = link.split("/")[-1]
        fighter_item["SHERDOG_FIGHTER_ID"] = sherdog_fighter_id
        fighter_item["TAPOLOGY_FIGHTER_ID"] = tapology_fighter_id

        yield fighter_item
