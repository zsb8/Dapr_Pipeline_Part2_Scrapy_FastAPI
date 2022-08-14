import platform
import time
import pandas as pd
import json
from fastapi import FastAPI, Response
from typing import Optional, Dict
from pydantic import BaseModel
from jobpostings import compare_jobs, jobs_to_postedjobs

app = FastAPI()
operation = platform.uname()


if operation[0] == "Linux":
    @app.get('/dapr/subscribe')
    async def subscribe(response: Response):
        # set content-type header to application/json
        print('Begin to judge router'.center(30, '-'), flush=True)
        response.headers["content-type"] = "application/json"
        subscriptions = [{'pubsubname': 'pubsubnoahs', 'topic': 'jobpostings', 'route': 'jobpostings'}]
        return subscriptions

    class Item(BaseModel):
        pubsubname: str
        traceid: str
        data: Dict[str, str]
        id: str
        datacontenttype: str
        source: str
        specversion: str
        type: str
        topic: str

    @app.post('/jobpostings')
    async def etl_subscriber(item: Item, response: Response):
        print('Begin jobpostings'.center(30, '-'), flush=True)
        data = item.data
        data_request = data['request']
        data_data = data['data']
        request = data_request
        data = data_data
        if request == "upsert" and data:
            df_web = pd.read_json(data, orient='columns')
            insert_count_jobs, modify_count_jobs = compare_jobs.main(df_web)
            result = ""
            result += f"Inserted jobs {insert_count_jobs} rows." if insert_count_jobs else "Not insert jobs."
            result += f"Modified jobs {modify_count_jobs} rows." if modify_count_jobs else "Not modify jobs."
            print('Begin to run jobs_to_postedjobs'.center(30, '-'), flush=True)
            begin_time = int(time.time())
            insert_count_postedjobs, modify_count_postedjobs = jobs_to_postedjobs.main()
            end_time = int(time.time())
            print(f"Now datetime is {time.ctime()}. Jobpostings cost  {round((end_time - begin_time) / 60, 2)} minutes.", flush=True)
            result += f"Inserted postedjobs {insert_count_postedjobs} rows." if insert_count_postedjobs \
                else "Not insert postedjobs."
            result += f"Modified postedjobs {modify_count_postedjobs} rows." if modify_count_postedjobs \
                else "Not modify postedjobs."
            results = {"result": result}
        else:
            results = {"result": "Please give me an order."}
        print(results, flush=True)
        print('Finish jobpostings'.center(30, '-'), flush=True)
        # response.headers["content-type"] = "application/json"
        # return results
        return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}


if operation[0] == 'Windows':
    # Only for test in my WIN10, these will be deleted in PROC evn.

    class Item(BaseModel):
        request: str
        data: Optional[str] = None

    @app.post('/jobpostings')
    async def update_item(item: Item):
        print('Begin jobpostings'.center(20, '-'), flush=True)
        request = item.request
        data = item.data
        if request == "upsert" and data:
            df_web = pd.read_json(data, orient='columns')
            insert_count_jobs, modify_count_jobs = compare_jobs.main(df_web)
            result = ""
            result += f"Inserted jobs {insert_count_jobs} rows." if insert_count_jobs else "Not insert jobs."
            result += f"Modified jobs {modify_count_jobs} rows." if modify_count_jobs else "Not modify jobs."
            print('Begin to run jobs_to_postedjobs'.center(30, '-'), flush=True)
            insert_count_postedjobs, modify_count_postedjobs = jobs_to_postedjobs.main()
            result += f"Inserted postedjobs {insert_count_postedjobs} rows." if insert_count_postedjobs \
                else "Not insert postedjobs."
            result += f"Modified postedjobs {modify_count_postedjobs} rows." if modify_count_postedjobs \
                else "Not modify postedjobs."
            results = {"result": result}
        else:
            results = {"result": "Please give me an order."}
        print(results, flush=True)
        print('Finish jobpostings'.center(30, '-'), flush=True)
        return results


@app.get('/')
def read_root():
    return {"Hello": "World, da-pipelines"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8100)
