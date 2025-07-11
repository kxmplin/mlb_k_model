import statsapi
from datetime import date

YEAR = date.today().year
teams = statsapi.get(
    'teams',
    {'sportIds': 1, 'activeStatus': 'Yes',
     'fields': 'teams,id,name,abbreviation'}
)['teams']

print(f"\nActive MLB teams for {YEAR}:")
for t in teams:
    print(f"  {t['abbreviation']:3}  ->  {t['name']} (ID {t['id']})")

print(f"\nTotal: {len(teams)} teams – API connection looks good!")
