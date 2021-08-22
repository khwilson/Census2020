from strenum import StrEnum


class SummaryLevel(StrEnum):
    STATE = "040"
    STATE_COUNTY = "050"
    STATE_COUNTY_TRACT = "140"
    STATE_COUNTY_TRACT_BLOCKGROUP = "150"
