import os
from mysql.connector import MySQLConnection
from mysql.connector import errorcode
from mysql.connector.errors import Error

QUERY_INSERT1 = '''INSERT INTO python_mysql.'''
QUERY_INSERT2 = ''' (`id`,`GMT_year`,`GMT_month`,`GMT_day`,`GMT_time`,
`local_year`,`local_month`,`local_day`,`local_time`,`number_time`,`time`,`time_zone_number`,`start_day`,`index_arch`,
`visibility_range`,`Qvr`,`Dvr`,`total_amount_of_clouds`,`Qtac`,`tac_lower_tier`,`Qtaclt`,`shape_cloud_up_tier`,
`Qscut`,`shape_cloud_middle_tier`,`Qscmt`,`sc_vertical_development`,`Qscvd`,`lcc`,`Qlcc`,`rc`,`Qrc`,`CBH`,`Qcbh`,`Dcbh`,
`cbsl`,`Qcbsl`,`ssc`,`Qssc`,`Dssc`,`wbd`,`Qwbd`,`wto`,`Qwto`,`dw`,`Qdw`,`msw`,`Qmsw`,`Dmsw`,`mxsw`,`Qmxsw`,`Dmxsw`,
`amount_precipiation`,`Qap`,`soil_surface_temperature`,`Qsst`,`sst_minimum_thermometer`,`Qsstmt`,`min_sst`,`Qmnsst`,
`max_sst`,`Qmxsst`,`ssmxtas`,`Qssmxtas`,`air_temperature_on_dry_term`,`Qatodt`,`air_temperature_on_wetted_term`,
`Qatowt`,`Datowt`,`atmnt`,`Qatmnt`,`min_at`,`Qmnat`,`max_at`,`Qmxat`,`atmxtas`,`Qatmxtas`,
`partial_water_vapor_pressure`,`Qpwvp`,`Q1pwvp`,`relative_humidity`,`Qrh`,`deficiency_of_water_vapor_saturation`,
`Qdwvs`,`Ddwvs`,`dpt`,`Qdpt`,`ap_station_lvl`,`Qapst`,`ap_sea_lvl`,`Qapsea`,`char_pressure_tendency`,`Qchprtd`,
`vol_prtd`,`Qvlprtd`)
VALUES
(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''

ARCH_SROK8C_DIR = '../Archives/Srok8c/Srok8c'
ARCH_ATM8C_DIR = '../Archives/Atm8c/Atm8c'
LENGTH_SROK8C = [0, 5, 10, 13, 16, 19, 24, 27, 30, 33, 35, 38, 41, 44, 46,
                 49, 51, 53, 56, 58, 61, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83, 88, 90, 92, 94, 96, 98, 100, 102,
                 104, 106, 109, 111, 115, 117, 120, 122, 124, 127, 129, 131, 138, 140, 146, 148, 154, 156, 162, 164,
                 170, 172, 178, 180, 186, 188, 194, 196, 198, 204, 206, 212, 214, 220, 222, 228, 230, 236, 238, 240,
                 244, 246, 253, 255, 257, 263, 265, 272, 274, 281, 283, 286, 288, 293, 295]


def insert_arch(mass, wmo_id):
    try:
        print("attempt to write to the table")
        cnx = MySQLConnection(user='scott', password='password', host='localhost', database='python_mysql')
        cnx.autocommit = False
        cursor = cnx.cursor()
        query = QUERY_INSERT1 + str(wmo_id) + QUERY_INSERT2
        cursor.executemany(query, mass)
        cnx.commit()
        if cnx.is_connected():
            print('connection established')
        else:
            print('connection failed')

    except Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    finally:
        cursor.close()
        cnx.close()
        print('connection closed.')


def pars_arch_srok8c():
    name_files = os.listdir(ARCH_SROK8C_DIR)
    tables = connector.show_tables()
    for name in name_files:
        with open(ARCH_SROK8C_DIR + '\\' + name) as file:
            if name[0:5] not in tables:
                connector.create_table(name[0:5])
            arr = []
            for row in file:
                arr1 = []
                for k in range(len(LENGTH_SROK8C) - 1):
                    elem = row[int(LENGTH_SROK8C[k]):int(LENGTH_SROK8C[k + 1])].strip()
                    if elem == "":
                        elem = None
                    arr1.append(elem)
                arr.append(arr1)
            insert_arch(arr, name[0:5])


def main():
    pars_arch_srok8c()


if __name__ == '__main__':
    main()