import pyodbc
import pandas as pd

class soldApartments:

    def __init__(self):
        self.driver= '{SQL Server}'
        self.server = 'DESKTOP-F0MM68K'
        self.database = 'christopherFuru'
        self.schema = 'Booli'
        self.table_name_apartments = 'SoldApartments'
        self.create_table_query = f"""
                        CREATE TABLE {self.schema + '.[' + self.table_name_apartments + ']'}
                        (
                            ApartmentId					INT IDENTITY(1,1) PRIMARY KEY,
                            GrowthFactor				DECIMAL(9,2) NOT NULL,
                            [Address]					VARCHAR(100) NOT NULL,
                            NumberOfRooms				DECIMAL(9,2) NOT NULL,
                            SizeInSquaredMeter			DECIMAL(9,2) NOT NULL,
                            ExtendedSizeInSquaredMeter	DECIMAL(9,2) NOT NULL,
                            Region						VARCHAR(100) NOT NULL,
                            InitialPriceInSek			MONEY NOT NULL,
                            PricePerSquaredMeterInSek	MONEY NOT NULL,
                            DateSold					DATE NOT NULL,
                            ObjectLink					VARCHAR(100) NOT NULL
                        );"""

        self.create_temp_table_query = f"""
                        CREATE TABLE #{self.table_name_apartments}
                        (
                            GrowthFactor				DECIMAL(9,2) NOT NULL,
                            [Address]					VARCHAR(100) NOT NULL,
                            NumberOfRooms				DECIMAL(9,2) NOT NULL,
                            SizeInSquaredMeter			DECIMAL(9,2) NOT NULL,
                            ExtendedSizeInSquaredMeter	DECIMAL(9,2) NOT NULL,
                            Region						VARCHAR(100) NOT NULL,
                            InitialPriceInSek			MONEY NOT NULL,
                            PricePerSquaredMeterInSek	MONEY NOT NULL,
                            DateSold					DATE NOT NULL,
                            ObjectLink					VARCHAR(100) NOT NULL
                        );"""

        self.query_merge = f"""
                        MERGE
                            {self.schema + '.[' + self.table_name_apartments + ']'}
                        AS
                            D
                        USING
                        (
                            SELECT * FROM #{self.table_name_apartments}
                        ) AS S ON
                            S.ObjectLink = D.ObjectLink
                        WHEN NOT MATCHED THEN INSERT
                        (
                            GrowthFactor,
                            [Address],
                            NumberOfRooms,
                            SizeInSquaredMeter,
                            ExtendedSizeInSquaredMeter,
                            Region,
                            InitialPriceInSek,
                            PricePerSquaredMeterInSek,
                            DateSold,
                            ObjectLink
                        )
                        VALUES
                        (
                            S.GrowthFactor,
                            S.[Address],
                            S.NumberOfRooms,
                            S.SizeInSquaredMeter,
                            S.ExtendedSizeInSquaredMeter,
                            S.Region,
                            S.InitialPriceInSek,
                            S.PricePerSquaredMeterInSek,
                            S.DateSold,
                            S.ObjectLink
                        );
        """

    def connect(self):
        cnxn = pyodbc.connect('DRIVER=' + self.driver + \
                            ';SERVER='+ self.server + \
                            ';DATABASE='+ self.database + \
                            ';Trusted_Connection=yes')
        cursor = cnxn.cursor()
        return cnxn, cursor

    def create_table(self, cnxn, cursor):

        try:
            cursor.execute(self.create_table_query)
            cursor.commit()
        except:
            print('Table already exists!')

    def merge(self, df, cnxn, cursor):
        cursor.execute(f"""IF OBJECT_ID(N'tempdb..#{self.table_name_apartments}') IS NOT NULL
                        DROP TABLE #{self.table_name_apartments};""")
        cursor.commit()

        cursor.execute(self.create_temp_table_query)
        cursor.commit()

        query_insert_into_temp_table = f"""
                        INSERT INTO #{self.table_name_apartments} VALUES 
                    """
        for i, item in enumerate(df.values.tolist()):
            query_insert_into_temp_table += "('" + \
                                            str(item[0]) + \
                                            "','" + str(item[1]) + \
                                            "','" + str(item[2]) +  \
                                            "','" + str(item[3]) +  \
                                            "','" + str(item[4]) +  \
                                            "','" + str(item[5]) +  \
                                            "','" + str(item[6]) +  \
                                            "','" + str(item[7]) +  \
                                            "','" + str(item[8]) +  \
                                            "','" + str(item[9]) +  \
                                            "')"
            if i < len(df.values.tolist())-1:
                query_insert_into_temp_table += ","
            else:
                query_insert_into_temp_table += ";"

        cursor.execute(query_insert_into_temp_table)
        cursor.commit()

        cursor.execute(self.query_merge)
        cursor.commit()

        print("SQL-merge done!")

class soldApartmentsDetails(soldApartments):
    
    def __init__(self):
        soldApartments.__init__(self)
        self.table_name_apartments_details = 'SoldApartmentsDetails'
        self.create_table_details_query = f"""
                        CREATE TABLE {self.schema + '.[' + self.table_name_apartments_details + ']'}
                        (
                            ApartmentDetailId		INT IDENTITY(1,1) PRIMARY KEY,
                            MonthlyChargeInSek		MONEY NULL,
                            OperatingCostInSek      MONEY NULL,
                            FloorNumber				DECIMAL(9,2) NULL,
                            BuiltYear				INT NULL,
                            Association				VARCHAR(100) NULL,
                            ObjectLink				VARCHAR(100) NOT NULL
                        );"""

        self.create_temp_table_details_query = f"""
                        CREATE TABLE #{self.table_name_apartments_details}
                        (
                            ApartmentDetailId		INT IDENTITY(1,1) PRIMARY KEY,
                            MonthlyChargeInSek		MONEY NULL,
                            OperatingCostInSek      MONEY NULL,
                            FloorNumber				DECIMAL(9,2) NULL,
                            BuiltYear				INT NULL,
                            Association				VARCHAR(100) NULL,
                            ObjectLink				VARCHAR(100) NOT NULL
                        );"""

        self.query_merge_details = f"""
                        MERGE
                            {self.schema + '.[' + self.table_name_apartments_details + ']'}
                        AS
                            D
                        USING
                        (
                            SELECT * FROM #{self.table_name_apartments_details}
                        ) AS S ON
                            S.ObjectLink = D.ObjectLink
                        WHEN NOT MATCHED THEN INSERT
                        (
                            MonthlyChargeInSek,
                            OperatingCostInSek,
                            FloorNumber,
                            BuiltYear,
                            Association,
                            ObjectLink
                        )
                        VALUES
                        (
                            S.MonthlyChargeInSek,
                            S.OperatingCostInSek,
                            S.FloorNumber,
                            S.BuiltYear,
                            S.Association,
                            S.ObjectLink
                        );"""

    def merge_details(self, df, cnxn, cursor):
        cursor.execute(f"""IF OBJECT_ID(N'tempdb..#{self.table_name_apartments_details}') IS NOT NULL
                        DROP TABLE #{self.table_name_apartments_details};""")
        cursor.commit()

        cursor.execute(self.create_temp_table_details_query)
        cursor.commit()

        query_insert_into_temp_table = f"""
                        INSERT INTO #{self.table_name_apartments_details} VALUES 
                    """
        for i, item in enumerate(df.values.tolist()):
            query_insert_into_temp_table += "(" + \
                                            str(item[0]) + \
                                            "," + str(item[3]) + \
                                            "," + str(item[5]) +  \
                                            "," + str(item[2]) +  \
                                            ",'" + str(item[1]) +  \
                                            "','" + str(item[4]) +  \
                                            "')"
            if i < len(df.values.tolist())-1:
                query_insert_into_temp_table += ","
            else:
                query_insert_into_temp_table += ";"

        cursor.execute(query_insert_into_temp_table)
        cursor.commit()

        cursor.execute(self.query_merge_details)
        cursor.commit()