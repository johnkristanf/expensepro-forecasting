import holidays 

def get_ph_holidays(years):
    return holidays.country_holidays(
        country="PH", years=years
    )