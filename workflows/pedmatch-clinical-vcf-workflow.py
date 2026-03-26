import os, pandas as pd
from typing import Literal
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from src.utils import get_time, file_dl, upload_folder_to_s3, get_run_logger, get_time, get_logger
from workflows.cnv import download_cnv, gistic_like_calls
import shutil
import math

# task to read in manifest file to dataframe and check columns
@task(name="read_manifest", log_prints=True)
def read_manifest(manifest_name: str) -> pd.DataFrame:
    """Read in manifest file to dataframe and check columns

    Args:
        manifest_name (str): Path to the manifest file

    Returns:
        pd.DataFrame: Dataframe of the manifest file
    """
    # read in manifest file
    manifest_df = pd.read_excel(manifest_name)

    # check if required columns are present
    required_columns = ["sample_id", "s3_url", "file_name", "md5sum", "file_size", "participant_id", "sample_type"]
    for col in required_columns:
        if col not in manifest_df.columns:
            raise ValueError(f"Missing required column: {col}")
    # check if there are any missing values in required columns
    for col in required_columns:
        if manifest_df[col].isnull().any():
            raise ValueError(f"Missing values in required column: {col}")
    # check if there are any duplicate sample_ids
    if manifest_df["sample_id"].duplicated().any():
        raise ValueError("Duplicate sample_ids found in manifest file")
    # check if there are any duplicate file_names
    if manifest_df["file_name"].duplicated().any():
        raise ValueError("Duplicate file_names found in manifest file")
    # check if there are any duplicate s3_urls
    if manifest_df["s3_url"].duplicated().any():
        raise ValueError("Duplicate s3_urls found in manifest file")
    # check if there are any duplicate md5sums
    if manifest_df["md5sum"].duplicated().any():
        raise ValueError("Duplicate md5sums found in manifest file")
    
    return manifest_df

DropDownChoices = Literal["parse_transform", "cleanup"]

VCF_HEADER_COLS = ["CHROM", "POS", "ID",  "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT"]

# task to read in file and prep pertinent cols
@task(name="clin_vcf_file_prep", log_prints=True)
def clin_vcf_file_prep(file_path: str, sample_id: str) -> pd.DataFrame:
    """Read in clinical VCF file and prep pertinent columns for annotation and merging to maf

    Args:
        file_path (str): Path to the clinical VCF file

    Returns:
        pd.DataFrame: Dataframe of the clinical variants with pertinent columns for annotation and merging to maf
    """
    # read in vcf file with pandas
    vcf_df = pd.read_csv(file_path, sep="\t", comment="#", names=VCF_HEADER_COLS+[sample_id], low_memory=False)
    
    # filter to PASS variants
    vcf_df = vcf_df[vcf_df["FILTER"] == "PASS"]
    
    return vcf_df

def copy_number_to_log2(observed_cn, baseline_cn=2):
    """
    Convert observed copy number to log2 copy number ratio.
    
    Parameters:
        observed_cn (float): observed copy number
        baseline_cn (float): baseline (normal) copy number, default = 2
        
    Returns:
        float: log2 copy number ratio
    """
    if observed_cn <= 0:
        raise ValueError("Observed copy number must be > 0")
    
    return math.log2(observed_cn / baseline_cn)

# task to format fusion data
@task(name="fusion_file_prep", log_prints=True)
def fusion_file_prep(input_df: pd.DataFrame, sample_id: str) -> pd.DataFrame:
    """Read in fusion file and prep pertinent columns for annotation and merging to maf"""
    
    fusion_df = input_df.copy()
    fusion_df = input_df[input_df['INFO'].str.contains("SVTYPE=Fusion")]
    
    if len(fusion_df) == 0:
        return pd.DataFrame(columns=["Sample_Id", "SV_Status", "Site1_Hugo_Symbol", "Site1_Region_Number", "Site2_Hugo_Symbol", "Site2_Region_Number", "NCBI_Build", "Class", "Method", "Event_Info", "Annotation", "DNA_Support", "RNA_Support", "Tumor_Read_Count", "Site1_Chromosome", "Site1_Position", "Site2_Chromosome", "Site2_Position"])
    
    # output array
    op = []
    
    # get uniq fusion ID by taking ID and removing _1 or _2 at the end 
    fusion_df.loc[:, 'FUSION_ID'] = fusion_df['ID'].str.split("_", expand=True).str[0]
    
    # get data  from INFO column; gene names are in the format GENE_NAME=<NAME>;
    fusion_df.loc[:, 'GENE'] = fusion_df['INFO'].str.extract(r'GENE_NAME=([^;]+)')
    fusion_df.loc[:, 'EXON'] = fusion_df['INFO'].str.extract(r'EXON_NUM=([^;]+)')
    fusion_df.loc[:, 'Annotation'] = fusion_df['INFO'].str.extract(r'ANNOTATION=([^;]+)')
    fusion_df.loc[:, 'Tumor_Read_Count'] = fusion_df['INFO'].str.extract(r'READ_COUNT=([^;]+)')
    
    for group_name, group_df in fusion_df.groupby("FUSION_ID"):
        
        # iterate in steps of 2
        for i in range(0, len(group_df), 2):
            pair = group_df.iloc[i:i+2]
            pair = pair.sort_index()
            #print(group_name)
            #print(pair.iloc[0]['EXON'])
            temp_dict = {
                "Sample_Id" : sample_id,
                "SV_Status": "SOMATIC",
                "Site1_Hugo_Symbol" : pair.iloc[0]['GENE'],
                "Site1_Region_Number": pair.iloc[0]['EXON'],
                "Site2_Hugo_Symbol" : pair.iloc[1]['GENE'],
                "Site2_Region_Number": pair.iloc[1]['EXON'],
                "NCBI_Build": "GRCh37",
                "Class": "Fusion",
                "Method" : "Oncomine",
                "Event_Info": group_name,
                "Annotation": pair.iloc[1]['Annotation'],
                "DNA_Support": "No",
                "RNA_Support": "Yes",
                "Tumor_Read_Count": pair.iloc[1]['Tumor_Read_Count'],
                "Site1_Chromosome" : pair.iloc[0]['CHROM'],
                "Site1_Position": pair.iloc[0]['POS'],
                "Site2_Chromosome" : pair.iloc[1]['CHROM'],
                "Site2_Position": pair.iloc[1]['POS'],
            }
            op.append(temp_dict)
        
    return pd.DataFrame(op)

# flow for fusion 
@flow(name="fusion_flow", log_prints=True)
def fusion_flow(tumor_input_df: pd.DataFrame, tumor_sample_id: str, normal_input_df: pd.DataFrame, normal_sample_id: str, logger) -> pd.DataFrame:
    """Flow to process fusion data from clinical VCFs for pedmatch"""
    
    tumor_fusion = fusion_file_prep(tumor_input_df, tumor_sample_id)
    normal_fusion = fusion_file_prep(normal_input_df, normal_sample_id)
    
    logger.info(f"{tumor_sample_id} Tumor fusion: {len(tumor_fusion)}")
    logger.info(f"{normal_sample_id} Normal fusion: {len(normal_fusion)}")
    
    if len(tumor_fusion) == 0: #no fusions found in tumor sample, return empty df with correct columns
        return tumor_fusion
    
    # remove from tumor fusion any fusions that are also in normal fusion based on Site1_Hugo_Symbol, Site2_Hugo_Symbol, Site1_Region_Number, and Site2_Region_Number
    merged_fusion = tumor_fusion.merge(normal_fusion, on=["Site1_Hugo_Symbol", "Site2_Hugo_Symbol", "Site1_Region_Number", "Site2_Region_Number"], how="left", indicator=True)
    merged_fusion.loc[merged_fusion["_merge"] == "both", "SV_Status"] = "GERMLINE"
    merged_fusion = merged_fusion.drop(columns=["_merge"])
    
    # remove GERMLINE fusions that are in tumor fusion from tumor fusion
    merged_fusion = merged_fusion[merged_fusion["SV_Status"] != "GERMLINE"]
    
    print(f"Somatic fusion after removing germline fusions for {tumor_sample_id}: {len(merged_fusion)}")
    logger.info(f"Somatic fusion after removing germline fusions for {tumor_sample_id}: {len(merged_fusion)}")
    
    return merged_fusion

# task for cnv file prep
@task(name="cnv_file_prep", log_prints=True)
def cnv_file_prep(input_df: pd.DataFrame, sample_id: str) -> pd.DataFrame:
    """Read in CNV file and prep pertinent columns for annotation and merging to maf"""
    
    cnv_df = input_df.copy()
    cnv_df = cnv_df[(cnv_df.ALT == "<CNV>") & (cnv_df.FILTER == 'PASS') & (cnv_df.INFO.str.contains('Amplification'))]
    
    if len(cnv_df) == 0:
        return pd.DataFrame(columns=["Sample_Id", "Patient_Id", "Hugo_Symbol", "chromosome", "start", "end", "num.mark", "seg.mean", "copy_number"])
    
    # get data  from INFO column
    cnv_df.loc[:, 'Num_Probes'] = cnv_df['INFO'].str.extract(r'NUMTILES=([^;]+)')
    cnv_df.loc[:, 'End'] = cnv_df['INFO'].str.extract(r'END=([^;]+)')
    cnv_df.loc[:, 'cn'] = cnv_df['INFO'].str.extract(r'RAW_CN=([^;]+)')
    cnv_df.loc[:, 'log2'] = cnv_df['cn'].apply(lambda x: copy_number_to_log2(float(x)))
    
    # op array
    op = []
    
    for _, row in cnv_df.iterrows():
        temp_dict = {
            "Sample_Id": sample_id,
            "Patient_Id": sample_id.split("_")[0],
            "Hugo_Symbol": row["ID"],
            "chromosome": row["CHROM"],
            "start": row["POS"],
            "end": row["End"],
            "num.mark": row["Num_Probes"],
            "seg.mean": row["log2"], ##
            "copy_number": row["cn"], ##
        }
        op.append(temp_dict)
    
    return pd.DataFrame(op)

# task to format cnv segment

# task to format cnv discrete

# task to format cnv log2 continuous

# flow for cnv
@flow(name="cnv_flow", log_prints=True)
def cnv_flow(tumor_vcf, tumor_sample_id, normal_vcf, normal_sample_id, logger) -> pd.DataFrame:
    """Flow to process CNV data from clinical VCFs for pedmatch"""
    
    tumor_cnv = cnv_file_prep(tumor_vcf, tumor_sample_id)
    normal_cnv = cnv_file_prep(normal_vcf, normal_sample_id)
    
    logger.info(f"{tumor_sample_id} Tumor CNV: {len(tumor_cnv)}")
    logger.info(f"{normal_sample_id} Normal CNV: {len(normal_cnv)}")
    
    if len(tumor_cnv) == 0: #no CNVs found in tumor sample, return empty df with correct columns
        return tumor_cnv
    
    # remove from tumor cnv any cnvs that are also in normal cnv based on hugo symbol
    merged_cnv = tumor_cnv.merge(normal_cnv, on=["Hugo_Symbol"], how="left", indicator=True)
    merged_cnv.loc[merged_cnv["_merge"] == "both", "SV_Status"] = "GERMLINE"
    merged_cnv = merged_cnv.drop(columns=["_merge"])
    
    # remove GERMLINE CNVs that are in tumor CNV from tumor CNV
    merged_cnv = merged_cnv[merged_cnv["SV_Status"] != "GERMLINE"]
    
    print(f"Somatic CNV after removing germline CNVs for {tumor_sample_id}: {len(merged_cnv)}")
    logger.info(f"Somatic CNV after removing germline CNVs for {tumor_sample_id}: {len(merged_cnv)}")
    
    return merged_cnv

# patient flow
@flow(name="pt_paired_vcf_flow_{tumor_sample_id}_{normal_sample_id}", log_prints=True)
def pt_paired_vcf_flow(tumor_vcf, tumor_sample_id, normal_vcf, normal_sample_id, logger):
    # prep files
    tumor_vcf_df = clin_vcf_file_prep(tumor_vcf, tumor_sample_id)
    normal_vcf_df = clin_vcf_file_prep(normal_vcf, normal_sample_id)
    
    # process fusion data
    fusion_results = fusion_flow(tumor_vcf_df, tumor_sample_id, normal_vcf_df, normal_sample_id, logger)
    
    return fusion_results


# batch flow
def batch_process(batch_df: pd.DataFrame, logger, runner_logger) -> None:
    
    # array of pd.DataFrames to hold fusion results for each tumor normal pair
    fusion_op = []
    
    # iterate thru tumor normal pairs
    for group_name, group_df in batch_df.groupby("participant_id"):
        runner_logger.info(f"Processing participant: {group_name}")
        tumor_df = group_df[group_df["sample_type"] == "tissue"]
        normal_df = group_df[group_df["sample_type"] == "blood"]
        if len(tumor_df) != 1 or len(normal_df) != 1:
            runner_logger.warning(f"Skipping participant {group_name} due to incorrect number of tumor or normal samples")
            logger.warning(f"Participant {group_name} tumor samples: {len(tumor_df)}, normal samples: {len(normal_df)}")
            continue
        tumor_sample_id = tumor_df.iloc[0]["sample_id"]
        normal_sample_id = normal_df.iloc[0]["sample_id"]
        
        try:
            fusion_results = pt_paired_vcf_flow(tumor_df.iloc[0]["file_name"], tumor_sample_id, normal_df.iloc[0]["file_name"], normal_sample_id, logger)
        except Exception as e:
            runner_logger.error(f"Error processing participant {group_name}: {e}")
            logger.error(f"Error processing participant {group_name}: {e}")
            continue
        
        # add to output array
        fusion_op.append(fusion_results)
    
    return fusion_op

# main flow:
@flow(name="pedmatch_clinical_vcf_flow", log_prints=True)
def pedmatch_clinical_vcf_flow(bucket: str, output_dir: str, manifest_path: str, flow_type: DropDownChoices):
    """Parse, format and annotate variants from clinical VCFs for pedmatch

    Args:
        bucket (str): s3 bucket to download/upload files from/to
        output_dir (str): path to directory to store intermediate and output files in s3
        manifest_path (str): path in s3 to the manifest file listing input VCFs; manifest headers are sample_id, sample_type, participant_id, file_name, s3_url and md5sum
        flow_type: str: whether to run the parse and transform steps or just the cleanup step to remove intermediate files; options are "parse_transform" or "cleanup"
    """
    
    dt = get_time()
    
    runner_logger = get_run_logger()
    
    if flow_type == "cleanup":
        
        # get a list of all dirs in /usr/local/data/pedmatch
        runner_logger.info(f"Cleaning up pedmatch_flow output directory")
        output_path = "/usr/local/data/pedmatch"
        dirs = os.listdir(output_path)
        # delete all dirs in /usr/local/data/pedmatch
        for dir in dirs:
            dir_path = os.path.join(output_path, dir)
            if os.path.isdir(dir_path):
                runner_logger.info(f"Deleting directory: {dir_path}")
                shutil.rmtree(dir_path)
                runner_logger.info(f"Deleted directory: {dir_path}")
            else:
                runner_logger.info(f"Skipping non-directory file: {dir_path}")
        runner_logger.info(f"Finished cleaning up pedmatch_flow output directory")
        return None
    
    runner_logger.info("Starting pedmatch clinical VCF workflow")
    
    # change working directory to mounted drive
    output_path = os.path.join("/usr/local/data/pedmatch", "pedmatch_run_"+dt)
    os.makedirs(output_path, exist_ok=True)
    
    # create logger
    log_filename = f"{output_path}/pedmatch_clinical_parse_transform"
    logger = get_logger(log_filename, "info")
    logger.info(f"Output path: {output_path}")
    
    # download manifest
    file_dl(bucket, manifest_path)    
    
    # read in manifest
    manifest_df = read_manifest(os.path.basename(manifest_path))
    
    # download files:
    # reusing download cnv function since it performs the same steps 
    # of downloading files from s3, checking md5sums and file sizes, and logging
    batch_size = 200 # temp for testing with 10 files; change to 500 for full run
    #for i in range(0, len(manifest_df), batch_size):
    for i in range(0, 200, batch_size): # temp for testing with 500 files
        batch_df = manifest_df.iloc[i:i+batch_size]
        runner_logger.info(f"Downloading batch {i//batch_size + 1} of {len(manifest_df)//batch_size + 1}")
        download_cnv(batch_df, logger)
    
    fusion_concat_results= []
    
    # add batch loop here
    for i in range(0, len(manifest_df.head(200)), batch_size):
        batch_df = manifest_df.iloc[i:i+batch_size]
        fusion_batch_op = batch_process(batch_df, logger, runner_logger)
        fusion_concat_results.extend(fusion_batch_op)
    
    # save output files
    fusion_output_path = os.path.join(output_path, "fusion_results.txt")
    
    pd.concat(fusion_concat_results).to_csv(fusion_output_path, sep="\t", index=False)
    
    # upload dir to s3
    upload_folder_to_s3(
            local_folder=output_path,
            bucket=bucket,
            destination=output_dir,
            sub_folder=""
        )

# parse files into constituent output files
# perform comparisons of tumor and normal samples to get somatic variants
# annotate clinical SNV variants with Genome Nexus API
# upload files to s3