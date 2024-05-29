import pandas as pd
import numpy as np
import pytest
from src.processing.cleaning import *

@pytest.fixture()
def arrange_test_data():
    columns = ['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location']
    rows = [
        [1  ,  "Jeremie"   ,  "Franey"     ,  "jeremie.franey@terrifictotes.com"   ,      "Sales",  "Manchester"    ],
        [  2  ,    "Deron" ,    "Beier"   ,       "deron.beier@terrifictotes.com"   ,   "Purchasing",  "Manchester"   ],
        [  3  , "Jeanette" ,   "Erdman"   ,   "jeanette.erdman@terrifictotes.com"   ,   "Production",       "Leeds"   ],
        [  4  ,      "Ana" ,   "Glover"   ,        "ana.glover@terrifictotes.com"   ,   "Dispatch",        "Leds"   ],
        [  5  ,"Magdalena" ,    "Zieme"   ,   "magdalena.zieme@terrifictotes.com"   ,   "Finance",  "Manchester"   ],
        [  6  ,    "Korey" ,  "Kreiger"   ,     "korey.kreiger@terrifictotes.com"   ,    "Facilities",  "Manchester"  ],
        [  7  ,  "Raphael" ,   "Rippin"   ,    "raphael.rippin@terrifictotes.com"   ,    "Communications",       "Leeds"  ],
        [  8  ,  "Oswaldo" ,"Bergstrom"   , "oswaldo.bergstrom@terrifictotes.com"   ,  "HR",       "Leeds"    ],
        [  9  ,    "Brody" ,    "Ratke"   ,       "brody.ratke@terrifictotes.com"   ,  "NaN",         "NaN"   ],
        [ 10  ,   "Jazmyn" ,     "Kuhn"   ,       "jazmyn.kuhn@terrifictotes.com"   ,  "NaN",         "NaN"   ],
        [  11 ,      "Meda",    "Cremin"  ,        "meda.cremin@terrifictotes.com"  ,  "NaN",         "NaN"   ],
        [  12 ,     "Imani",    "Walker"  ,       "imani.walker@terrifictotes.com"  ,  "NaN",         "NaN"   ],
        [  13 ,      "Stan",    "Lehner"  ,        "stan.lehner@terrifictotes.com"  ,  "NaN",         "NaN"   ],
        [  14 , "Rigoberto", "VonRueden"  ,"rigoberto.vonrueden@terrifictotes.com"  ,  "NaN",         "NaN"   ],
        [  15 ,       "Tom", "Gutkowski"  ,      "tom.gutkowski@terrifictotes.com"  ,  "NaN",         "NaN"   ],
        [  16 ,      "Jett",  "Parisian"  ,      "jett.parisian@terrifictotes.com"  ,  "NaN",         "NaN"   ],
        [  17 ,    "Irving",   "O'Keefe"  ,     "irving.o'keefe@terrifictotes.com"  ,  "NaN",         "NaN"   ],
        [  18 ,    "Tomasa",     "Moore"  ,       "tomasa.moore@terrifictotes.com"  ,  "NaN",         "NaN"   ],
        [  19 ,    "Pierre",     "Sauer"  ,       "pierre.sauer@terrifictotes.com"  ,  "NaN",         "NaN"   ],
        [  20 ,    "Flavio",     "Kulas"  ,       "flavio.kulas@terrifictotes.com"  ,  "NaN",         "NaN"   ]
    ]
    test_df = pd.DataFrame(np.array(rows), columns=columns)
    return test_df

class TestCleaning:
    def test_cleaning_func(self,arrange_test_data):
        print(arrange_test_data)
        clean_data(
            table_name = 'dim_staff',
            table_df = arrange_test_data,
            schema_requirements = schema_requirements
            )