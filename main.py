from doctest import Example
from fastapi import FastAPI, Depends
from core.database import Database
from geojson import Feature, FeatureCollection, loads
from fastapi_simple_security import api_key_router, api_key_security

app = FastAPI()

app.include_router(api_key_router, prefix="/auth", tags=["_auth"])

@app.get("/", dependencies=[Depends(api_key_security)])
def root():
    return {"message": "GIS API running"}


@app.get("/geometries", dependencies=[Depends(api_key_security)])
async def get_geometries():
    """
    Return all geometries in database as GeoJSON
    """
    db = Database()
    try:
        db.execute("""select ST_AsGeojson(ST_AsText(the_geom)), code from public.postal_codes""")
        geometries = db.fetchall()
        geojson = FeatureCollection([Feature(geometry=loads(geometry[0]), properties={"zipcode": geometry[1]}) for geometry in geometries])
        return {"data": geojson}
    except Exception as e:
        return {"error": str(e)}
    finally:
        db.close()

# Create an endpoint that return data from postgres database
@app.get("/feature_info/{coordinates}", dependencies=[Depends(api_key_security)])
async def get_feature_info(coordinates: str):
    """
    Return a list of features that are within the given coordinates.
    """
    db = Database()
    try :
        coord = list(map(float, tuple(coordinates.split(","))))
        query = f"""SELECT SUM(P.amount::numeric) as "Amount", P.p_age as "Age", P.p_gender as "Gender", MIN(C.code) as "ZIPCODE" FROM public.paystats P INNER JOIN public.postal_codes C on P.postal_code_id = C.id WHERE ST_Contains(ST_AsText(the_geom), ST_GeomFromText('POINT({coord[0]} {coord[1]})')) GROUP BY "Gender", "Age" ORDER BY "Age";"""
        result = db.execute(query)
        cursor = db.cur
        items = [dict(zip([key[0] for key in cursor.description], row)) for row in db.fetchall()]

        # Return json data
        return {"data": items}
    except Exception as e:
        return {"error": str(e)}
    finally:
        db.close()


@app.get("/total_amount/{start_date}/{end_date}", dependencies=[Depends(api_key_security)])
async def get_total_amount(start_date: str, end_date: str):
    """
    Return the total turnover between the given dates.
    """
    db = Database()
    try :
        query = f"""SELECT SUM(P.amount::numeric) as "Amount" FROM public.paystats P INNER JOIN public.postal_codes C on P.postal_code_id = C.id WHERE P.p_month BETWEEN '{start_date}' AND '{end_date}';"""
        db.execute(query)       
        cursor = db.cur
        items = [dict(zip([key[0] for key in cursor.description], row)) for row in db.fetchall()]

        # Return json data
        return {"data": items}
    except Exception as e:
        return {"error": str(e)}
    finally:
        db.close()


@app.get("/total_amount_age/{start_date}/{end_date}", dependencies=[Depends(api_key_security)])
async def get_total_amount(start_date: str, end_date: str):
    """
    Return the total turnover between the given dates by age and gender.
    """
    db = Database()
    try :
        query = f"""SELECT P.p_age as "Age", P.p_gender as "Gender", SUM(P.amount::numeric) as "Amount" FROM public.paystats P INNER JOIN public.postal_codes C on P.postal_code_id = C.id WHERE P.p_month BETWEEN '{start_date}' AND '{end_date}' GROUP BY "Age", "Gender";"""
        db.execute(query)       
        cursor = db.cur
        items = [dict(zip([key[0] for key in cursor.description], row)) for row in db.fetchall()]

        # Return json data
        return {"data": items}
    except Exception as e:
        return {"error": str(e)}
    finally:
        db.close()


@app.get("/total_amount_month/{start_date}/{end_date}", dependencies=[Depends(api_key_security)])
async def get_total_amount(start_date: str, end_date: str):
    """
    Return the total turnover between the given dates by month and gender.
    """
    db = Database()
    try :
        query = f"""SELECT P.p_month as "Month", P.p_gender as "Gender", SUM(P.amount::numeric) as "Amount" FROM public.paystats P INNER JOIN public.postal_codes C on P.postal_code_id = C.id WHERE P.p_month BETWEEN '{start_date}' AND '{end_date}' GROUP BY "Month", "Gender";"""
        db.execute(query)       
        cursor = db.cur
        items = [dict(zip([key[0] for key in cursor.description], row)) for row in db.fetchall()]

        # Return json data
        return {"data": items}
    except Exception as e:
        return {"error": str(e)}
    finally:
        db.close()


@app.get("/geometry_union", dependencies=[Depends(api_key_security)])
async def get_geometries():
    """
    Return part of Comunidad de Madrid’s geometry as an union of all Madrid’s postal codes, including aggregated paystat data for the whole geometry as a property
    """
    db = Database()
    try:
        # Get Comunidad de Madrid's geometry using materialized view
        db.execute("""SELECT * FROM public.geometry_amount""")
        geometries = db.fetchall()
        geojson = FeatureCollection([Feature(geometry=loads(geometry[0]), properties={"Total_amount": geometry[1]}) for geometry in geometries])
        return {"data": geojson}
    except Exception as e:
        return {"error": str(e)}
    finally:
        db.close()



