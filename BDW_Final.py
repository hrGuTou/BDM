import re

from pyspark import SparkContext

year2015 = '/data/share/bdm/nyc_parking_violation/2015.csv'
year2016 = '/data/share/bdm/nyc_parking_violation/2016.csv'
year2017 =  '/data/share/bdm/nyc_parking_violation/2017.csv'
year2018 = '/data/share/bdm/nyc_parking_violation/2018.csv'
year2019 = '/data/share/bdm/nyc_parking_violation/2019.csv'
nyc_cscl = '/data/share/bdm/nyc_cscl.csv'

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


def exactViolation(partId, records):
    if partId == 0:
        # skip header
        next(records)

    import csv
    reader = csv.reader(records)
    for row in reader:
        if len(row) < 25:
            continue
        if len(row[4].split('/')) < 3:
            continue
        (house, street, county, year) = (str(row[23]), str(row[24]), str(row[21]), int(row[4].split('/')[2]))

        if year in (2015, 2016, 2017, 2018, 2019):

            if not house or re.search('[a-zA-Z]', house):
                continue
            house = re.findall(r"[\w']+", house)
            if len(house) > 2 or len(house) < 1:
                continue
            if county in BOROUGH:
                if len(house) > 1:
                    if int(house[1]) % 2 == 0:
                        side = 'R'
                    else:
                        side = 'L'
                else:
                    if int(house[0]) % 2 == 0:
                        side = 'R'
                    else:
                        side = 'L'
                yield (street.upper(), BOROUGH[county], tuple(map(int, house)), 'v'), (
                'v', side, tuple(map(int, house)), year)


def exactCSCL(partId, records):
    if partId == 0:
        # skip header
        next(records)

    import csv
    reader = csv.reader(records)
    for row in reader:
        (physicalID, fullSt, stLabel, boCode, L_LOW_HN, L_HIGH_HN, R_LOW_HN, R_HIGH_HN) = (
            row[0], row[28], row[10], int(row[13]), row[2].split('-'), row[3].split('-'), row[4].split('-'),
            row[5].split('-'))
        if boCode in BOROCODE:
            if physicalID.isdigit():
                if fullSt == stLabel:
                    if L_LOW_HN[0]:
                        yield (fullSt.upper(), BOROCODE[boCode], tuple(map(int, L_LOW_HN)), 'c'), (
                        'c', physicalID, 'L', tuple(map(int, L_LOW_HN)))
                    if L_HIGH_HN[0]:
                        yield (fullSt.upper(), BOROCODE[boCode], tuple(map(int, L_HIGH_HN)), 'c'), (
                        'c', physicalID, 'L', tuple(map(int, L_HIGH_HN)))
                    if R_LOW_HN[0]:
                        yield (fullSt.upper(), BOROCODE[boCode], tuple(map(int, R_LOW_HN)), 'c'), (
                        'c', physicalID, 'R', tuple(map(int, R_LOW_HN)))
                    if R_HIGH_HN[0]:
                        yield (fullSt.upper(), BOROCODE[boCode], tuple(map(int, R_HIGH_HN)), 'c'), (
                        'c', physicalID, 'R', tuple(map(int, R_HIGH_HN)))
                else:
                    if L_LOW_HN[0]:
                        if stLabel:
                            yield (stLabel.upper(), BOROCODE[boCode], tuple(map(int, L_LOW_HN)), 'c'), (
                            'c', physicalID, 'L', tuple(map(int, L_LOW_HN)))
                        if fullSt:
                            yield (fullSt.upper(), BOROCODE[boCode], tuple(map(int, L_LOW_HN)), 'c'), (
                            'c', physicalID, 'L', tuple(map(int, L_LOW_HN)))
                    if L_HIGH_HN[0]:
                        if stLabel:
                            yield (stLabel.upper(), BOROCODE[boCode], tuple(map(int, L_HIGH_HN)), 'c'), (
                            'c', physicalID, 'L', tuple(map(int, L_HIGH_HN)))
                        if fullSt:
                            yield (fullSt.upper(), BOROCODE[boCode], tuple(map(int, L_HIGH_HN)), 'c'), (
                            'c', physicalID, 'L', tuple(map(int, L_HIGH_HN)))
                    if R_LOW_HN[0]:
                        if stLabel:
                            yield (stLabel.upper(), BOROCODE[boCode], tuple(map(int, R_LOW_HN)), 'c'), (
                            'c', physicalID, 'R', tuple(map(int, R_LOW_HN)))
                        if fullSt:
                            yield (fullSt.upper(), BOROCODE[boCode], tuple(map(int, R_LOW_HN)), 'c'), (
                            'c', physicalID, 'R', tuple(map(int, R_LOW_HN)))
                    if R_HIGH_HN[0]:
                        if stLabel:
                            yield (stLabel.upper(), BOROCODE[boCode], tuple(map(int, R_HIGH_HN)), 'c'), (
                            'c', physicalID, 'R', tuple(map(int, R_HIGH_HN)))
                        if fullSt:
                            yield (fullSt.upper(), BOROCODE[boCode], tuple(map(int, R_HIGH_HN)), 'c'), (
                            'c', physicalID, 'R', tuple(map(int, R_HIGH_HN)))


def filterAndCount(records):
    res = [0, 0, 0, 0, 0]
    houseRanges = [-1, -1, -1, -1]  # initialize range (0L low, 1L high, 2R low,3 R high)
    lastLCSCL = None
    lastRCSCL = None
    for k, v in records:
        # print(k, v)
        if v[0] == 'v':
            side = v[1]
            if side == 'L':
                if houseRanges[0] != -1:
                    res[v[-1] - 2015] += 1

                    if lastLCSCL[0][0:3] != k[0:3]:
                        # new bound, return and reset
                        # print(res)
                        yield lastLCSCL[1][1], res
                        lastLCSCL = None
                        res = [0, 0, 0, 0, 0]
                        houseRanges[0] = -1
                        houseRanges[2] = -1
                        continue

            else:
                if houseRanges[2] != -1:
                    res[v[-1] - 2015] += 1

                    if lastRCSCL != k[0:3]:
                        # new bound, return and reset
                        yield lastRCSCL[1][1], res
                        lastRCSCL = None
                        res = [0, 0, 0, 0, 0]
                        houseRanges[0] = -1
                        houseRanges[2] = -1
                        continue

        if v[0] == 'c':
            # cscl data
            # check if bound is set, if no then set
            side = v[2]
            if side == 'L':
                # check left low boound
                if houseRanges[0] == -1:
                    # set L low bound
                    houseRanges[0] = v[-1]
                    lastLCSCL = (k, v)

                else:
                    # set L high bound
                    houseRanges[1] = v[-1]
                    lastLCSCL = (k, v)
            else:
                if houseRanges[2] == -1:
                    # set R low bound
                    houseRanges[2] = v[-1]
                    lastRCSCL = (k, v)

                else:
                    # set R high bound
                    houseRanges[3] = v[-1]
                    lastRCSCL = (k, v)

if __name__ == '__main__':
    sc = SparkContext()

    violation2015 = sc.textFile(year2015, use_unicode=True)
    violation2016 = sc.textFile(year2016, use_unicode=True)
    violation2017 = sc.textFile(year2017, use_unicode=True)
    violation2018 = sc.textFile(year2018, use_unicode=True)
    violation2019 = sc.textFile(year2019, use_unicode=True)

    violationData = violation2015.union(violation2016).union(violation2017).union(violation2018).union(violation2019)

    csclData = sc.textFile(nyc_cscl, use_unicode=True)

    res_violation = violationData.mapPartitionsWithIndex(exactViolation)
    res_cscl = csclData.mapPartitionsWithIndex(exactCSCL)

    res_union = res_violation.union(res_cscl)
    res_union = res_union.sortByKey()

    output = res_union.mapPartitions(filterAndCount).map(lambda x: (int(x[0]), x[1])) \
        .reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4]]) \
        .sortByKey() \
        .take(20)

    # output = res_filtered.collect()

    for i in output:
        print(i)
