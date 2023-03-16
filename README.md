# Data Access Module

This Module is to take user's requests to download data from various sources.
This Downloaded data can be accessed and from a SSD location.

User Input type:
1. User can provide csv files with vins and corresponding date - number of months for which the data to be sownloaded.
2. User can also give various tage filters, and the program will randomly select vehicles id which satisfy the criteria upto a max of certain number of vehicle.

Filter types:
1. Date range
2. Specific VINs with date
3. Tag filters for vehicle selection (telematic name, bs norm, fuel type, vehicle model, engine series, manufacturing year & month, etc..)
4. Value filter for fetching data for certain value of a field
5. field selection, for optimal selection of the required fields only

Usage:
The downloaded files can be used in different use cases like analytical services, monitoring, etc..
