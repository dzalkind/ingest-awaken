import json
import sys
import datetime
from os import listdir
from os.path import isfile, join
import os


def generate_header_string():
    return (
        "json_file,date_time,duration_sec,num_blobs,size_min_pix,size_max_pix,x_min_m,x_max_m,y_min_m,y_max_m,z_min_m,z_max_m\n")


def parse_json_tracks(input_directory):
    print("Running track_summary over all .json files in: " + input_directory)
    ofilename = os.path.basename(input_directory) + '-tracks.csv'
    print("Opening output file: " + join(input_directory, ofilename))
    output_file = open(join(input_directory, ofilename), 'w')
    output_file.write(generate_header_string())

    # load the .json tracks3d files
    for f in listdir(input_directory):
        if (f.endswith(".json")):
            print(f)
            datafile = open(join(input_directory, f), 'r')

            # the json library parses the json files into lists for us
            data = json.load(datafile)

            # the timestamps are in milliseconds
            blob0_timestamp = data['blobs'][0]['cam1_blob']['timestamp'] / 1000
            blob0_time = datetime.datetime.fromtimestamp(blob0_timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
            # print(blob0_time)

            duration_sec = (data['endTime'] - data['startTime']) / 1000
            # print(duration_sec)

            num_blobs = len(data['blobs'])
            # print(num_blobs)

            size_min_pix = -999
            size_max_pix = -999
            x_min_m = -999
            y_min_m = -999
            z_min_m = -999
            x_max_m = -999
            y_max_m = -999
            z_max_m = -999

            for blob in data['blobs']:
                # cam1 and cam2 keep track of their blobs separately
                # use min and max from both for the overall min/max values
                if (blob['cam1_blob']['image'] is not None):
                    cam1_pixels = sum(x > 0 for x in blob['cam1_blob']['image'])
                else:
                    cam1_pixels = 0
                if (blob['cam2_blob']['image'] is not None):
                    cam2_pixels = sum(x > 0 for x in blob['cam2_blob']['image'])
                else:
                    cam2_pixels = 0
                blob_min_pixels = min(cam1_pixels, cam2_pixels)
                blob_max_pixels = max(cam1_pixels, cam2_pixels)

                if (size_min_pix == -999 or blob_min_pixels < size_min_pix):
                    size_min_pix = blob_min_pixels
                if (size_max_pix == -999 or blob_max_pixels > size_max_pix):
                    size_max_pix = blob_max_pixels
                if (x_min_m == -999 or blob['x'] < x_min_m):
                    x_min_m = blob['x']
                if (y_min_m == -999 or blob['y'] < y_min_m):
                    y_min_m = blob['y']
                if (z_min_m == -999 or blob['z'] < z_min_m):
                    z_min_m = blob['z']
                if (x_max_m == -999 or blob['x'] > x_max_m):
                    x_max_m = blob['x']
                if (y_max_m == -999 or blob['y'] > y_max_m):
                    y_max_m = blob['y']
                if (z_max_m == -999 or blob['z'] > z_max_m):
                    z_max_m = blob['z']

            # print(size_min_pix)
            # print(size_max_pix)
            # print(x_min_m)
            # print(x_max_m)
            # print(y_min_m)
            # print(y_max_m)
            # print(z_min_m)
            # print(z_max_m)

            output_file.write(f + "," +
                              str(blob0_time) + "," +
                              str(duration_sec) + "," +
                              str(num_blobs) + "," +
                              str(size_min_pix) + "," +
                              str(size_max_pix) + "," +
                              str(x_min_m) + "," +
                              str(x_max_m) + "," +
                              str(y_min_m) + "," +
                              str(y_max_m) + "," +
                              str(z_min_m) + "," +
                              str(z_max_m) + "\n")

    output_file.close()

    # Return the path to the csv file
    return join(input_directory, ofilename)


if __name__ == "__main__":
    parse_json_tracks(sys.argv[1])