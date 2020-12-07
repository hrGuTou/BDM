from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import re
from pyspark.sql import functions as F
from pyspark.sql.types import *

SAMPLECSV = 'hdfs:///data/share/bdm/nyc_parking_violations/2015.csv'
CSCLCSV = 'hdfs:///data/share/bdm/nyc_cscl.csv'

BOROCODE = {
    1: 'Manhattan',
    2: 'Bronx',
    3: 'Brooklyn',
    4: 'Queens',
    5: 'Staten Island',
}

BOROUGH = {
    'MAN': 'Manhattan',
    'MH': 'Manhattan',
    'MN': 'Manhattan',
    'NEWY': 'Manhattan',
    'NEW Y': 'Manhattan',
    'NY': 'Manhattan',
    'BRONX': 'Bronx',
    'BX': 'Bronx',
    'BK': 'Brooklyn',
    'K': 'Brooklyn',
    'KING': 'Brooklyn',
    'KINGS': 'Brooklyn',
    'Q': 'Queens',
    'QN': 'Queens',
    'QNS': 'Queens',
    'QU': 'Queens',
    'QUEEN': 'Queens',
    'R': 'Staten Island',
    'RICHMOND': 'Staten Island'
}

def exactViolation(idx, records):
    if idx == 0:
        next(records)

    import csv
    reader = csv.reader(records)
    for row in reader:
        (house, street, county, year) = (str(row[23]), str(row[24]), str(row[21]), int(row[4].split('/')[2]))

        if not house or re.search('[a-zA-Z]', house):
            continue

        house = re.findall(r"[\w']+", house)

        # if len(house)>2:
        #     continue

        if county in BOROUGH:
            if len(house)>1:
                if int(house[1])%2 == 0:
                    side = 'R'
                else:
                    side = 'L'
            else:
                if int(house[0])%2 == 0:
                    side = 'R'
                else:
                    side = 'L'
            yield Row(StreetName=street.upper(), Borough=BOROUGH[county], HouseNumber=list(map(int, house)), Side=side, Year=year)


def exactCSCL(idx, records):
    if idx == 0:
        next(records)

    import csv
    reader = csv.reader(records)
    for row in reader:
        (physicalID, fullSt, stLabel, boCode, L_LOW_HN, L_HIGH_HN, R_LOW_HN, R_HIGH_HN) = (
            row[0], row[28], row[10], int(row[13]), row[2], row[3], row[4], row[5])

        if boCode in BOROCODE:
            if L_LOW_HN and L_HIGH_HN and R_LOW_HN and R_HIGH_HN:
                yield Row(PhysicalID=physicalID, FullStreet=fullSt.upper(), StreetLabel=stLabel, Borough=BOROCODE[boCode], L=[L_LOW_HN, L_HIGH_HN], R=[R_LOW_HN,R_HIGH_HN])


@F.udf(returnType=BooleanType())
def houseNumInRange(house, side, Lrange, Rrange):
    Left_low = list(map(int,Lrange[0].split('-')))
    Left_high = list(map(int,Lrange[1].split('-')))
    Right_low = list(map(int,Rrange[0].split('-')))
    Right_high = list(map(int,Rrange[1].split('-')))


    if len(house) == 1:
        # single number
        if side == 'L':
            #left side
            if not len(Left_low) == 1:
                return False
            return Left_low[0] <= house[0] <= Left_high[0]
        else:
            #right side
            if not len(Right_low) == 1:
                return False
            return Right_low[0] <= house[0] <= Right_high[0]
    else:
        # compound number
        if side == 'L':
            if not len(Left_low) == 2:
                return False
            return Left_low[0] <= house[0] <= Left_high[0] and Left_low[1] <= house[1] <= Left_high[1]
        else:
            if not len(Right_low) == 2:
                return False
            return Left_low[0] <= house[0] <= Left_high[0] and Left_low[1] <= house[1] <= Left_high[1]

if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()


    violationData = spark.sparkContext.textFile(SAMPLECSV).mapPartitionsWithIndex(exactViolation)
    csclData = spark.sparkContext.textFile(CSCLCSV).mapPartitionsWithIndex(exactCSCL)

    violationSchema = StructType([StructField('StreetName', StringType()),
                         StructField('Borough', StringType()),
                         StructField('HouseNumber', ArrayType(IntegerType())),
                         StructField('Side', StringType()),
                         StructField('Year', IntegerType())
                         ])

    csclSchema = StructType([StructField('PhysicalID', StringType()),
                             StructField('FullStreet', StringType()),
                             StructField('StreetLabel', StringType()),
                             StructField('Borough', StringType()),
                             StructField('L', ArrayType(StringType())),
                             StructField('R', ArrayType(StringType()))
                             ])



    df_violation = sqlContext.createDataFrame(violationData, violationSchema)
    df_cscl = sqlContext.createDataFrame(csclData, csclSchema)

    #res = df_violation.filter(df_violation.Year==2015)
    #print(res.collect())
    #df_cscl.show()


    df_res = df_violation.join(df_cscl, [(df_violation.Borough==df_cscl.Borough) & ((df_violation.StreetName==df_cscl.FullStreet) | (df_violation.StreetName==df_cscl.StreetLabel))], how='left')

    df_res = df_res.filter(houseNumInRange(df_res.HouseNumber, df_res.Side, df_res.L, df_res.R))
    df_res = df_res.filter(df_res.Year==2015)

    print(df_res.count())