SELECT
	A.Address,
	RD.BuiltYear,
	RD.MonthlyChargeInSek,
	RD.OperatingCostInSek,
	RD.FloorNumber,
	RD.NumberOfRooms,
	RD.SizeInSquaredMeter,
	RD.ExtendedSizeInSquaredMeter,
	D.District,
	RD.InitialPriceInSek,
	RD.PricePerSquaredMeterInSek,
	RD.SoldPriceInSek,
	RD.SoldPricePerSquaredMeterInSek,
	RD.DateSold
FROM
	christopherFuru.Booli.FactSoldApartments RD
	JOIN christopherFuru.Booli.DimAddress A ON
		A.AddressId = RD.AddressId
	JOIN christopherFuru.Booli.DimDistrict D ON
		D.DistrictId = RD.DistrictId;