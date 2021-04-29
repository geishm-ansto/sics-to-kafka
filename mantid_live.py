"""
Provides are command interface to be able to issue start and stop commands
to Mantid via message through the Kafka topic
"""
import time
import argparse
import flatbuffers

from sicsclient.kafkahelp import (KafkaProducer, timestamp_to_msecs,
                                  create_runstart_message, publish_message)
import sicsclient.pyschema.RunStart as RunStart
import sicsclient.pyschema.RunStop as RunStop


def send_start_command(broker, topic, run_name, instrument):
    timestamp_ms = timestamp_to_msecs(time.time())
    producer = KafkaProducer(bootstrap_servers=[broker])

    file_identifier = b"pl72"
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True) # only diff with ESS 
    run_name_ = builder.CreateString(run_name)
    instrument_ = builder.CreateString(instrument)
    nexus_struct_ = builder.CreateString("")
    RunStart.RunStartStart(builder)
    RunStart.RunStartAddRunName(builder, run_name_)
    RunStart.RunStartAddInstrumentName(builder, instrument_)
    RunStart.RunStartAddNexusStructure(builder, nexus_struct_)  # added because expecting 
    RunStart.RunStartAddStartTime(
        builder, timestamp_ms * 1000000)  # msec to nsec
    msg = RunStart.RunStartEnd(builder)
    builder.Finish(msg, file_identifier=file_identifier)
    publish_message(producer, topic, timestamp_ms, bytes(builder.Output()))


def send_stop_command(broker, topic, run_name):
    timestamp_ms = timestamp_to_msecs(time.time())
    producer = KafkaProducer(bootstrap_servers=[broker])

    file_identifier = b"6s4t"
    builder = flatbuffers.Builder(1024)
    run_name_ = builder.CreateString(run_name)
    RunStop.RunStopStart(builder)
    RunStop.RunStopAddRunName(builder, run_name_)
    RunStop.RunStopAddStopTime(
        builder, timestamp_ms * 1000000)  # msec to nsec
    msg = RunStop.RunStopEnd(builder)
    builder.Finish(msg, file_identifier=file_identifier)
    publish_message(producer, topic, timestamp_ms, bytes(builder.Output()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Send Start/Stop messages to Mantid via Kafka')
    parser.add_argument(
        '--broker', help='The Kafka Broker to connect to IP:P', default='localhost:9092')
    parser.add_argument(
        '--run', help='Run name, used as workspace name', default='1')
    parser.add_argument(
        '--instrument', help='Instrument name', default='PELICAN')
    parser.add_argument(
        '--start', help='Send a start message', action="store_true")
    parser.add_argument(
        '--stop', help='Send a stop message', action="store_true")
    parser.add_argument(
        '--topic', help='Publish to Kafka topic', default='run_mantid')
    args = parser.parse_args()

    if args.start and args.stop:
        raise ValueError('Which is it --stop or --start?')

    if args.start:
        send_start_command(args.broker, args.topic, args.run, args.instrument)
    elif args.stop:
        send_stop_command(args.broker, args.topic, args.run)
    else:
        print('Select either --stop or --start')
