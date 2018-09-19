import boto3
from datetime import datetime
from functools import reduce
import csv

client = boto3.client('xray')

def main():
    start_process()

def start_process():
    response = client.get_trace_summaries(StartTime=datetime(2018,9,19,5,25), EndTime=datetime(2018,9,19,5,59))
    traces = response["TraceSummaries"]
    if traces is not None and len(list(traces)) > 0:
        trace_ids =[]
        for trace in traces:
            trace_ids.append(trace["Id"])
        buckets = gather_data(trace_ids)
        write_to_files(buckets)


def gather_data(trace_ids):
    buckets = {}
    for trace_id in trace_ids:
            trace_details = client.get_trace_graph(TraceIds=[trace_id])
            services = trace_details["Services"]
            if services is not None and len(services)>0:
                for service in services:
                    service_name = service["Name"]
                    service_type = service.get("Type")
                    bucket_name = "{}_{}_{}".format(str(len(trace_ids)),service_type,  service_name)
                    bucket = buckets.get(bucket_name)
                    if bucket is None:
                        bucket = {}
                        bucket["data"] = []
                    data = bucket["data"]
                    element = {}
                    if service.get("SummaryStatistics") is not None:
                        element["total-response-time"]  = service.get("SummaryStatistics")["TotalResponseTime"]
                        element["type"] = service_type
                        element["reference-id"] = service.get("ReferenceId")
                        if service.get("DurationHistogram") is not None:
                            element["duration-time-hist"] = reduce(lambda x, y: x + y["Value"],service.get("DurationHistogram"),0 )
                        if service.get("ResponseTimeHistogram") is not None:
                            element["response-time-hist"] = reduce(lambda x, y: x + y["Value"],service.get("ResponseTimeHistogram"),0)
                        if element["response-time-hist"] is not None and element["duration-time-hist"] is not None:
                            element["init-cost"] = element["duration-time-hist"] - element["response-time-hist"]
                        data.append(element)
                    buckets[bucket_name] = bucket
    return buckets

def write_to_files(buckets):
    for bucket_name, bucket in buckets.items():
        file_name = "{}/{}.csv".format("data",str(bucket_name).replace("/","-"))
        with open(file_name, "w") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            writer.writerow(["total-response-time", "type", "duration-time-hist", "response-time-hist", "init-cost"])
            data = bucket.get("data")
            for element in data:
                writer.writerow([element["total-response-time"], element["type"],element["duration-time-hist"], element["response-time-hist"], element["init-cost"]])


if __name__ == '__main__':
    main()