# Successful Unit Conversions
The conversion factor is applied to the reported value from the source to obtain a value with standardized units.

| SOURCE PARAMETER NAME | SOURCE UNITS | DIE PARAMETER NAME | DIE PARAMETER UNITS | CONVERSION FACTOR | NOTES |
| :-------------------- | :----------- | :----------------- | :------------------ | :---------------- | :---- |
| -                     | m            | -                  | ft                  | 3.28084           | Applies to all records where source and die parameter names are equivalent |
| -                     | ft           | -                  | m                   | 0.3048            | Applies to all records where source and die parameter names are equivalent |
| -                     | ppm          | -                  | mg/L                | 1                 | Applies to all records where source and die parameter names are equivalent |
| -                     | ug/L         | -                  | mg/L                | 0.001             | Applies to all records where source and die parameter names are equivalent |
| -                     | ton/ac-ft    | -                  | mg/L                | 735.47            | Applies to all records where source and die parameter names are equivalent |
| bicarbonate           | mg/L as CaCO3 | bicarbonate       | mg/L                | 1.22              | equivalent mass HCO3 - = 61 |
| calcium               | mg/L as CaCO3 | calcium           | mg/L                | 0.4               | equivalent mass Ca 2+ = 20 |
| carbonate             | mg/L as CaCO3 | carbonate         | mg/L                | 0.6               | equivalent mass CO3 2- = 30 |
| nitrate as n          | mg/L as N    | nitrate            | mg/L                | 4.4268            | -     |
| nitrate               | mg/L as N    | nitrate            | mg/L                | 4.4268            | -     |
| nitrate               | ug/L as N    | nitrate            | mg/L                | 0.0044268         | -     |
| nitrate               | mg/L as NO3  | nitrate            | mg/L                | 1                 | -     |
| sulfate as SO4        | mg/L         | sulfate            | mg/L                | 1                 | -     |
| sulfur sulfate        | mg/L         | sulfate            | mg/L                | 1                 | -     |
| uranium               | pCi/L        | uranium            | mg/L                | 0.00149           | [conversion factor source](https://www.epa.gov/sites/default/files/2015-09/documents/qa_rad_webcast.pdf) |

## Converting from mg/L as CaCO3 to mg/L

```
mg/L as CaCO3 = mg/L * (equivalent mass CaCO3/equivalent mass analyte)

mg/L = mg/L as CaCO3 * (equivalent mass analyte/equivalent mass CaCO3)
```

where **equivalent mass analyte = atomic mass analyte/valency analyte**

so **equivalent mass CaCO3 = (equivalent mass Ca 2+) + (equivalent mass CO3 2-) = 40/2 + (12+3*16)/2 = 50**


## Converting from mg/L as N to mg/L

```
mg/L * (molecular weight N/molecular weight NO3) = mg/L as N 
mg/L = mg/L as N * (molecuar weight NO3/molecular weight N)
```

where **molecular weight N = 14.007 g/mol** and **molecular weight NO3 = (14.007+15.999*3) = 62.004 g/mol**

## Converting from pCi/L to mg/L

```
ug/L = pCi/L * 1.49
```

# Skipped Unit Conversions

Records with the following source parameter names, source units, die parameter names, and die parameter units are not included in the output.

| SOURCE PARAMETER NAME | SOURCE UNITS | NOTES |
| :-------------------- | :----------- | :---- |
| -                     | tons/day     | This is not a unit of concentration |
| -                     | mg/kg        | This is not a unit of aqueous concentration |