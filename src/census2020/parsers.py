import tempfile
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv

from .types import FilenameType

GEO_FIELDS = {
    "FILEID": pa.string(),
    "STUSAB": pa.string(),
    "SUMLEV": pa.string(),
    "GEOVAR": pa.string(),
    "GEOCOMP": pa.string(),
    "CHARITER": pa.string(),
    "CIFSN": pa.string(),
    "LOGRECNO": pa.int64(),
    "GEOID": pa.string(),
    "GEOCODE": pa.string(),
    "REGION": pa.string(),
    "DIVISION": pa.string(),
    "STATE": pa.string(),
    "STATENS": pa.string(),
    "COUNTY": pa.string(),
    "COUNTYCC": pa.string(),
    "COUNTYNS": pa.string(),
    "COUSUB": pa.string(),
    "COUSUBCC": pa.string(),
    "COUSUBNS": pa.string(),
    "SUBMCD": pa.string(),
    "SUBMCDCC": pa.string(),
    "SUBMCDNS": pa.string(),
    "ESTATE": pa.string(),
    "ESTATECC": pa.string(),
    "ESTATENS": pa.string(),
    "CONCIT": pa.string(),
    "CONCITCC": pa.string(),
    "CONCITNS": pa.string(),
    "PLACE": pa.string(),
    "PLACECC": pa.string(),
    "PLACENS": pa.string(),
    "TRACT": pa.string(),
    "BLKGRP": pa.string(),
    "BLOCK": pa.string(),
    "AIANHH": pa.string(),
    "AIHHTLI": pa.string(),
    "AIANHHFP": pa.string(),
    "AIANHHCC": pa.string(),
    "AIANHHNS": pa.string(),
    "AITS": pa.string(),
    "AITSFP": pa.string(),
    "AITSCC": pa.string(),
    "AITSNS": pa.string(),
    "TTRACT": pa.string(),
    "TBLKGRP": pa.string(),
    "ANRC": pa.string(),
    "ANRCCC": pa.string(),
    "ANRCNS": pa.string(),
    "CBSA": pa.string(),
    "MEMI": pa.string(),
    "CSA": pa.string(),
    "METDIV": pa.string(),
    "NECTA": pa.string(),
    "NMEMI": pa.string(),
    "CNECTA": pa.string(),
    "NECTADIV": pa.string(),
    "CBSAPCI": pa.string(),
    "NECTAPCI": pa.string(),
    "UA": pa.string(),
    "UATYPE": pa.string(),
    "UR": pa.string(),
    "CD116": pa.string(),
    "CD118": pa.string(),
    "CD119": pa.string(),
    "CD120": pa.string(),
    "CD121": pa.string(),
    "SLDU18": pa.string(),
    "SLDU22": pa.string(),
    "SLDU24": pa.string(),
    "SLDU26": pa.string(),
    "SLDU28": pa.string(),
    "SLDL18": pa.string(),
    "SLDL22": pa.string(),
    "SLDL24": pa.string(),
    "SLDL26": pa.string(),
    "SLDL28": pa.string(),
    "VTD": pa.string(),
    "VTDI": pa.string(),
    "ZCTA": pa.string(),
    "SDELM": pa.string(),
    "SDSEC": pa.string(),
    "SDUNI": pa.string(),
    "PUMA": pa.string(),
    "AREALAND": pa.int64(),
    "AREAWATR": pa.int64(),
    "BASENAME": pa.string(),
    "NAME": pa.string(),
    "FUNCSTAT": pa.string(),
    "GCUNI": pa.string(),
    "POP100": pa.int64(),
    "HU100": pa.int64(),
    "INTPTLAT": pa.string(),
    "INTPTLON": pa.string(),
    "LSADC": pa.string(),
    "PARTFLAG": pa.string(),
    "UGA": pa.string(),
}

PART1_FIELDS = {
    "FILEID": pa.string(),
    "STUSAB": pa.string(),
    "CHARITER": pa.string(),
    "CIFSN": pa.string(),
    "LOGRECNO": pa.int64(),
    "P0010001": pa.int64(),
    "P0010002": pa.int64(),
    "P0010003": pa.int64(),
    "P0010004": pa.int64(),
    "P0010005": pa.int64(),
    "P0010006": pa.int64(),
    "P0010007": pa.int64(),
    "P0010008": pa.int64(),
    "P0010009": pa.int64(),
    "P0010010": pa.int64(),
    "P0010011": pa.int64(),
    "P0010012": pa.int64(),
    "P0010013": pa.int64(),
    "P0010014": pa.int64(),
    "P0010015": pa.int64(),
    "P0010016": pa.int64(),
    "P0010017": pa.int64(),
    "P0010018": pa.int64(),
    "P0010019": pa.int64(),
    "P0010020": pa.int64(),
    "P0010021": pa.int64(),
    "P0010022": pa.int64(),
    "P0010023": pa.int64(),
    "P0010024": pa.int64(),
    "P0010025": pa.int64(),
    "P0010026": pa.int64(),
    "P0010027": pa.int64(),
    "P0010028": pa.int64(),
    "P0010029": pa.int64(),
    "P0010030": pa.int64(),
    "P0010031": pa.int64(),
    "P0010032": pa.int64(),
    "P0010033": pa.int64(),
    "P0010034": pa.int64(),
    "P0010035": pa.int64(),
    "P0010036": pa.int64(),
    "P0010037": pa.int64(),
    "P0010038": pa.int64(),
    "P0010039": pa.int64(),
    "P0010040": pa.int64(),
    "P0010041": pa.int64(),
    "P0010042": pa.int64(),
    "P0010043": pa.int64(),
    "P0010044": pa.int64(),
    "P0010045": pa.int64(),
    "P0010046": pa.int64(),
    "P0010047": pa.int64(),
    "P0010048": pa.int64(),
    "P0010049": pa.int64(),
    "P0010050": pa.int64(),
    "P0010051": pa.int64(),
    "P0010052": pa.int64(),
    "P0010053": pa.int64(),
    "P0010054": pa.int64(),
    "P0010055": pa.int64(),
    "P0010056": pa.int64(),
    "P0010057": pa.int64(),
    "P0010058": pa.int64(),
    "P0010059": pa.int64(),
    "P0010060": pa.int64(),
    "P0010061": pa.int64(),
    "P0010062": pa.int64(),
    "P0010063": pa.int64(),
    "P0010064": pa.int64(),
    "P0010065": pa.int64(),
    "P0010066": pa.int64(),
    "P0010067": pa.int64(),
    "P0010068": pa.int64(),
    "P0010069": pa.int64(),
    "P0010070": pa.int64(),
    "P0010071": pa.int64(),
    "P0020001": pa.int64(),
    "P0020002": pa.int64(),
    "P0020003": pa.int64(),
    "P0020004": pa.int64(),
    "P0020005": pa.int64(),
    "P0020006": pa.int64(),
    "P0020007": pa.int64(),
    "P0020008": pa.int64(),
    "P0020009": pa.int64(),
    "P0020010": pa.int64(),
    "P0020011": pa.int64(),
    "P0020012": pa.int64(),
    "P0020013": pa.int64(),
    "P0020014": pa.int64(),
    "P0020015": pa.int64(),
    "P0020016": pa.int64(),
    "P0020017": pa.int64(),
    "P0020018": pa.int64(),
    "P0020019": pa.int64(),
    "P0020020": pa.int64(),
    "P0020021": pa.int64(),
    "P0020022": pa.int64(),
    "P0020023": pa.int64(),
    "P0020024": pa.int64(),
    "P0020025": pa.int64(),
    "P0020026": pa.int64(),
    "P0020027": pa.int64(),
    "P0020028": pa.int64(),
    "P0020029": pa.int64(),
    "P0020030": pa.int64(),
    "P0020031": pa.int64(),
    "P0020032": pa.int64(),
    "P0020033": pa.int64(),
    "P0020034": pa.int64(),
    "P0020035": pa.int64(),
    "P0020036": pa.int64(),
    "P0020037": pa.int64(),
    "P0020038": pa.int64(),
    "P0020039": pa.int64(),
    "P0020040": pa.int64(),
    "P0020041": pa.int64(),
    "P0020042": pa.int64(),
    "P0020043": pa.int64(),
    "P0020044": pa.int64(),
    "P0020045": pa.int64(),
    "P0020046": pa.int64(),
    "P0020047": pa.int64(),
    "P0020048": pa.int64(),
    "P0020049": pa.int64(),
    "P0020050": pa.int64(),
    "P0020051": pa.int64(),
    "P0020052": pa.int64(),
    "P0020053": pa.int64(),
    "P0020054": pa.int64(),
    "P0020055": pa.int64(),
    "P0020056": pa.int64(),
    "P0020057": pa.int64(),
    "P0020058": pa.int64(),
    "P0020059": pa.int64(),
    "P0020060": pa.int64(),
    "P0020061": pa.int64(),
    "P0020062": pa.int64(),
    "P0020063": pa.int64(),
    "P0020064": pa.int64(),
    "P0020065": pa.int64(),
    "P0020066": pa.int64(),
    "P0020067": pa.int64(),
    "P0020068": pa.int64(),
    "P0020069": pa.int64(),
    "P0020070": pa.int64(),
    "P0020071": pa.int64(),
    "P0020072": pa.int64(),
    "P0020073": pa.int64(),
}


PART2_FIELDS = {
    "FILEID": pa.string(),
    "STUSAB": pa.string(),
    "CHARITER": pa.string(),
    "CIFSN": pa.string(),
    "LOGRECNO": pa.int64(),
    "P0030001": pa.int64(),
    "P0030002": pa.int64(),
    "P0030003": pa.int64(),
    "P0030004": pa.int64(),
    "P0030005": pa.int64(),
    "P0030006": pa.int64(),
    "P0030007": pa.int64(),
    "P0030008": pa.int64(),
    "P0030009": pa.int64(),
    "P0030010": pa.int64(),
    "P0030011": pa.int64(),
    "P0030012": pa.int64(),
    "P0030013": pa.int64(),
    "P0030014": pa.int64(),
    "P0030015": pa.int64(),
    "P0030016": pa.int64(),
    "P0030017": pa.int64(),
    "P0030018": pa.int64(),
    "P0030019": pa.int64(),
    "P0030020": pa.int64(),
    "P0030021": pa.int64(),
    "P0030022": pa.int64(),
    "P0030023": pa.int64(),
    "P0030024": pa.int64(),
    "P0030025": pa.int64(),
    "P0030026": pa.int64(),
    "P0030027": pa.int64(),
    "P0030028": pa.int64(),
    "P0030029": pa.int64(),
    "P0030030": pa.int64(),
    "P0030031": pa.int64(),
    "P0030032": pa.int64(),
    "P0030033": pa.int64(),
    "P0030034": pa.int64(),
    "P0030035": pa.int64(),
    "P0030036": pa.int64(),
    "P0030037": pa.int64(),
    "P0030038": pa.int64(),
    "P0030039": pa.int64(),
    "P0030040": pa.int64(),
    "P0030041": pa.int64(),
    "P0030042": pa.int64(),
    "P0030043": pa.int64(),
    "P0030044": pa.int64(),
    "P0030045": pa.int64(),
    "P0030046": pa.int64(),
    "P0030047": pa.int64(),
    "P0030048": pa.int64(),
    "P0030049": pa.int64(),
    "P0030050": pa.int64(),
    "P0030051": pa.int64(),
    "P0030052": pa.int64(),
    "P0030053": pa.int64(),
    "P0030054": pa.int64(),
    "P0030055": pa.int64(),
    "P0030056": pa.int64(),
    "P0030057": pa.int64(),
    "P0030058": pa.int64(),
    "P0030059": pa.int64(),
    "P0030060": pa.int64(),
    "P0030061": pa.int64(),
    "P0030062": pa.int64(),
    "P0030063": pa.int64(),
    "P0030064": pa.int64(),
    "P0030065": pa.int64(),
    "P0030066": pa.int64(),
    "P0030067": pa.int64(),
    "P0030068": pa.int64(),
    "P0030069": pa.int64(),
    "P0030070": pa.int64(),
    "P0030071": pa.int64(),
    "P0040001": pa.int64(),
    "P0040002": pa.int64(),
    "P0040003": pa.int64(),
    "P0040004": pa.int64(),
    "P0040005": pa.int64(),
    "P0040006": pa.int64(),
    "P0040007": pa.int64(),
    "P0040008": pa.int64(),
    "P0040009": pa.int64(),
    "P0040010": pa.int64(),
    "P0040011": pa.int64(),
    "P0040012": pa.int64(),
    "P0040013": pa.int64(),
    "P0040014": pa.int64(),
    "P0040015": pa.int64(),
    "P0040016": pa.int64(),
    "P0040017": pa.int64(),
    "P0040018": pa.int64(),
    "P0040019": pa.int64(),
    "P0040020": pa.int64(),
    "P0040021": pa.int64(),
    "P0040022": pa.int64(),
    "P0040023": pa.int64(),
    "P0040024": pa.int64(),
    "P0040025": pa.int64(),
    "P0040026": pa.int64(),
    "P0040027": pa.int64(),
    "P0040028": pa.int64(),
    "P0040029": pa.int64(),
    "P0040030": pa.int64(),
    "P0040031": pa.int64(),
    "P0040032": pa.int64(),
    "P0040033": pa.int64(),
    "P0040034": pa.int64(),
    "P0040035": pa.int64(),
    "P0040036": pa.int64(),
    "P0040037": pa.int64(),
    "P0040038": pa.int64(),
    "P0040039": pa.int64(),
    "P0040040": pa.int64(),
    "P0040041": pa.int64(),
    "P0040042": pa.int64(),
    "P0040043": pa.int64(),
    "P0040044": pa.int64(),
    "P0040045": pa.int64(),
    "P0040046": pa.int64(),
    "P0040047": pa.int64(),
    "P0040048": pa.int64(),
    "P0040049": pa.int64(),
    "P0040050": pa.int64(),
    "P0040051": pa.int64(),
    "P0040052": pa.int64(),
    "P0040053": pa.int64(),
    "P0040054": pa.int64(),
    "P0040055": pa.int64(),
    "P0040056": pa.int64(),
    "P0040057": pa.int64(),
    "P0040058": pa.int64(),
    "P0040059": pa.int64(),
    "P0040060": pa.int64(),
    "P0040061": pa.int64(),
    "P0040062": pa.int64(),
    "P0040063": pa.int64(),
    "P0040064": pa.int64(),
    "P0040065": pa.int64(),
    "P0040066": pa.int64(),
    "P0040067": pa.int64(),
    "P0040068": pa.int64(),
    "P0040069": pa.int64(),
    "P0040070": pa.int64(),
    "P0040071": pa.int64(),
    "P0040072": pa.int64(),
    "P0040073": pa.int64(),
    "H0010001": pa.int64(),
    "H0010002": pa.int64(),
    "H0010003": pa.int64(),
}

PART3_FIELDS = {
    "FILEID": pa.string(),
    "STUSAB": pa.string(),
    "CHARITER": pa.string(),
    "CIFSN": pa.string(),
    "LOGRECNO": pa.int64(),
    "P0050001": pa.int64(),
    "P0050002": pa.int64(),
    "P0050003": pa.int64(),
    "P0050004": pa.int64(),
    "P0050005": pa.int64(),
    "P0050006": pa.int64(),
    "P0050007": pa.int64(),
    "P0050008": pa.int64(),
    "P0050009": pa.int64(),
    "P0050010": pa.int64(),
}


def parse_census_part1(filename: FilenameType) -> pa.Table:
    """
    Parse a Part 1 table from the Census Bureau

    Args:
        filename: The file (should end in 12020.pl)

    Returns:
        A pyarrow Table version of the data
    """
    return pcsv.read_csv(
        filename,
        read_options=pa.csv.ReadOptions(
            column_names=list(PART1_FIELDS), encoding="latin1"
        ),
        parse_options=pcsv.ParseOptions(delimiter="|"),
        convert_options=pcsv.ConvertOptions(column_types=PART1_FIELDS),
    )


def parse_census_part2(filename: FilenameType) -> pa.Table:
    """
    Parse a Part 2 table from the Census Bureau

    Args:
        filename: The file (should end in 22020.pl)

    Returns:
        A pyarrow Table version of the data
    """
    return pcsv.read_csv(
        filename,
        read_options=pa.csv.ReadOptions(
            column_names=list(PART2_FIELDS), encoding="latin1"
        ),
        parse_options=pcsv.ParseOptions(delimiter="|"),
        convert_options=pcsv.ConvertOptions(column_types=PART2_FIELDS),
    )


def parse_census_part3(filename: FilenameType) -> pa.Table:
    """
    Parse a Part 3 table from the Census Bureau

    Args:
        filename: The file (should end in 32020.pl)

    Returns:
        A pyarrow Table version of the data
    """
    return pcsv.read_csv(
        filename,
        read_options=pa.csv.ReadOptions(
            column_names=list(PART3_FIELDS), encoding="latin1"
        ),
        parse_options=pcsv.ParseOptions(delimiter="|"),
        convert_options=pcsv.ConvertOptions(column_types=PART3_FIELDS),
    )


def parse_census_geo(filename: FilenameType) -> pa.Table:
    """
    Parse a geo table from the Census Bureau

    Args:
        filename: The file (should end in geo2020.pl)

    Returns:
        A pyarrow Table version of the data
    """
    return pcsv.read_csv(
        filename,
        read_options=pa.csv.ReadOptions(
            column_names=list(GEO_FIELDS), encoding="latin1"
        ),
        parse_options=pcsv.ParseOptions(delimiter="|"),
        convert_options=pcsv.ConvertOptions(column_types=GEO_FIELDS),
    )


def parse_all(filename: FilenameType) -> pa.Table:
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        with zipfile.ZipFile(filename, "r") as zfile:
            zfile.extractall(tmpdir)

        table1 = parse_census_part1(list(tmpdir.rglob("*12020.pl"))[0])
        table2 = parse_census_part2(list(tmpdir.rglob("*22020.pl"))[0])
        table3 = parse_census_part3(list(tmpdir.rglob("*32020.pl"))[0])
        tablegeo = parse_census_geo(list(tmpdir.rglob("*geo2020.pl"))[0])

    return combine_tables(table1, table2, table3, tablegeo)


def combine_tables(
    table1: pa.Table, table2: pa.Table, table3: pa.Table, tablegeo: pa.Table
) -> pa.Table:
    """
    Combine all four Census tables into a single table
    """
    # Verify everything is sorted
    table1 = table1.take(pc.sort_indices(table1["LOGRECNO"]))
    table2 = table2.take(pc.sort_indices(table2["LOGRECNO"]))
    table3 = table3.take(pc.sort_indices(table3["LOGRECNO"]))
    tablegeo = tablegeo.take(pc.sort_indices(tablegeo["LOGRECNO"]))

    # Remove common columns
    table1 = table1.drop(["LOGRECNO", "STUSAB", "FILEID", "CHARITER", "CIFSN"])
    table2 = table2.drop(["LOGRECNO", "STUSAB", "FILEID", "CHARITER", "CIFSN"])
    table3 = table3.drop(["LOGRECNO", "STUSAB", "FILEID", "CHARITER", "CIFSN"])

    # Create final schema
    final_schema = tablegeo.schema
    for field in table1.schema:
        final_schema = final_schema.append(field)
    for field in table2.schema:
        final_schema = final_schema.append(field)
    for field in table3.schema:
        final_schema = final_schema.append(field)

    # Create final table
    final_table = pa.Table.from_arrays(
        [*tablegeo.columns, *table1.columns, *table2.columns, *table3.columns],
        schema=final_schema,
    )

    return final_table
