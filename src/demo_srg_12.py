import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow import fs

import pandas as pd

import logging

from nzshm_common.location import location, coded_location

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

AWS_DATASET = 'ths-poc-arrow-test/SRWG_12_LOCS/'
LOCAL_DATASET = "../toshi-hazard-store/WORKING/ARROW/SRWG_12_LOCS"
SWRG_12_LOCS = ['Auckland', 'Blenheim', 'Christchurch', 'Dunedin', 'Gisborne', 'Greymouth', 'Masterton', 'Napier', 'Nelson', 'Queenstown', 'Tauranga', 'Wellington']


def srg_location_codes():
    return [loc['id'] for loc in location.LOCATIONS if (loc['name'] in SWRG_12_LOCS and loc['id'][:3] == "srg")]

def nz_location_codes():
    return [loc['id'] for loc in location.LOCATIONS if (loc['name'] in SWRG_12_LOCS and len(loc['id']) == 3)]

def binned_srg_locations():
    user_locations = location.get_locations(srg_location_codes())
    return coded_location.bin_locations(user_locations, 1.0)


# 8 secs for LOCAL, 58s for AWS
filesystem=fs.S3FileSystem(region='ap-southeast-2')
dataset = ds.dataset(
    LOCAL_DATASET,
    # AWS_DATASET,
    format='parquet',
    partitioning='hive',
    # filesystem=filesystem
)


def get_dataframe(dataset, partition_code, location):
    log.info(f'filtering dataset for partition: {partition_code} location: {location}')
    flt1 = (
        (pc.field('nloc_0') == pc.scalar(partition_code)) &\
        (pc.field('nloc_001') == pc.scalar(loc.code)) &\
        (pc.field('vs30') == pc.scalar(275))
    )
    columns = ['sources_digest', 'gmms_digest', 'nloc_001', 'values', 'imt', 'vs30'] # limit the data
    scanner = ds.Scanner.from_dataset(dataset, filter=flt1, columns=columns)
    tbl = scanner.to_table() # reads the dataset into memory
    return tbl.to_pandas()

frames = []
for partition_code, partition_bin in  binned_srg_locations().items():
    for loc in partition_bin.locations:
        df = get_dataframe(dataset, partition_code, loc)
        print(df)
        frames.append(df)


all_df = pd.concat(frames)
print(all_df)
assert 0


## Bonus time
from nzshm_model import branch_registry
registry = branch_registry.Registry()

registry.gmm_registry.get_by_hash('380a95154af2')
registry.source_registry.get_by_hash('6f53044d346a')

# for label in tbl['gmms_digest'].unique():
#     print(label)

# def inspect_table(tbl):

#     # table inspection
#     # help(tbl)

#     # table shape
#     tbl.shape

#     # column_names
#     tbl.column_names

#     # imt
#     for label in tbl['imt'].unique():
#         print(label)

# # gmms digests

# # for pandas users
# df0 = tbl.to_pandas()
# df0
# df0[df0.imt=='PGA']



