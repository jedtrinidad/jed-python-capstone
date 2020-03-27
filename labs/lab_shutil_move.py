import os
import shutil

path = '/home/ubuntu/environment/projects/kungfu/'

print("Before moving file:")
print(os.listdir(path))

source = path + 'source/'

destination = path + 'destination/'

dest = shutil.move(source, destination)

print("After moving file:")
print(os.listdir(path))

print("Destination path:", dest)