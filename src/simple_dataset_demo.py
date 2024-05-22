# pyarrow NSHM dataset demo
"""
Until the bucket is public read, the user must have an Authorsied S

NB the required bucket policy is:

{
    "Version": "2012-10-17",
    "Id": "Policy1716082928967",
    "Statement": [
        {
            "Sid": "statement1716082928967",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:GetObjectAttributes"
            ],
            "Resource": [
                "arn:aws:s3:::ths-poc-arrow-test/*",
                "arn:aws:s3:::ths-poc-arrow-test"
            ]
        }
    ]
}

"""
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow import fs

from nzshm_common.location import location, coded_location



filesystem=fs.S3FileSystem(region='ap-southeast-2')
dataset = ds.dataset(
    'ths-poc-arrow-test/THS_R4_DEFRAG/',
    format='parquet',
    partitioning='hive',
    filesystem=filesystem
)

# #option A
coded_location = location.get_locations(['WLG', 'MRO'])[0]
flt1 = (
    (pc.field('nloc_0') == pc.scalar(coded_location.downsample(1.0).code)) &\
    (pc.field('nloc_001') == pc.scalar(coded_location.code)) &\
    (pc.field('vs30') == pc.scalar(275))
)

# Option B (Faster for cloud, has a small nloc0 bin)
flt1 = (
    (pc.field('nloc_0') == pc.scalar("-34.0~173.0")) &\
    (pc.field('nloc_001') == pc.scalar('-34.500~172.600')) &\
    (pc.field('vs30') == pc.scalar(275))
)

columns = ['sources_digest', 'gmms_digest', 'nloc_001', 'values', 'imt', 'vs30'] # limit the data
scanner = ds.Scanner.from_dataset(dataset, filter=flt1, columns=columns)
tbl = scanner.to_table() # reads the dataset into memory

# table inspection
tbl
help(tbl)

# table shape
tbl.shape

# column_names
tbl.column_names

# imt
for label in tbl['imt'].unique():
    print(label)

# gmms digests
for label in tbl['gmms_digest'].unique():
    print(label)

# for pandas users
df0 = tbl.to_pandas()
df0
df0[df0.imt=='PGA']


## Bonus time
from nzshm_model import branch_registry
registry = branch_registry.Registry()

registry.gmm_registry.get_by_hash('380a95154af2')
registry.source_registry.get_by_hash('6f53044d346a')

