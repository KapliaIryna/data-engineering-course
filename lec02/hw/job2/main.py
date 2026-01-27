from dotenv import load_dotenv

from flask import Flask, request
from flask import typing as flask_typing
from bll.convert_to_avro import convert_sales_to_avro

load_dotenv()
app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    
    try:
        input_data: dict = request.json
        
        if not input_data:
            return {"message": "Request body must be JSON"}, 400
        
        date = input_data.get('date')
        raw_dir = input_data.get('raw_dir')
        stg_dir = input_data.get('stg_dir')

        # check
        if not date:
            return {"message": "date parameter is required"}, 400
        
        if not raw_dir:
            return {"message": "raw_dir parameter is required"}, 400
        
        if not stg_dir:
            return {"message": "stg_dir parameter is required"}, 400

        # conversion
        print(f"\n{'-'*40}")
        print(f"Received request: date={date}")
        print(f"  raw_dir={raw_dir}")
        print(f"  stg_dir={stg_dir}")
        print(f"{'-'*40}\n")
        
        records_count = convert_sales_to_avro(
            date=date,
            raw_dir=raw_dir,
            stg_dir=stg_dir
        )

        return {
            "message": "Data converted successfully to Avro",
            "records_count": records_count,
            "date": date
        }, 201
    
    except FileNotFoundError as e:
        print(f"ERROR: {str(e)}")
        return {"message": f"File not found: {str(e)}"}, 404
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return {"message": f"Error: {str(e)}"}, 500


if __name__ == "__main__":
    print("\n" + "-"*40)
    print("Starting Job2 Flask Server")
    print("Port: 8082")
    print("Endpoint: POST http://localhost:8082/")
    print("-"*40 + "\n")
    
    app.run(debug=True, host="localhost", port=8082)