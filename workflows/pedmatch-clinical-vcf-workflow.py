import os, pandas as pd
import csv
from typing import Literal
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from src.utils import get_time, file_dl, upload_folder_to_s3, get_run_logger, get_time, get_logger
from workflows.cnv import download_cnv, gistic_like_calls
import shutil
import math
from prefect_shell import ShellOperation
from workflows.vcf_anno import install_nexus, annotator, concat_mafs
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
    
    return round(math.log2(observed_cn / baseline_cn), 4)

# task to format fusion data
@task(name="fusion_file_prep", log_prints=True)
def fusion_file_prep(input_df: pd.DataFrame, sample_id: str) -> pd.DataFrame:
    """Read in fusion file and prep pertinent columns for annotation and merging to maf"""
    
    # Check if required columns exist
    required_cols = ['INFO', 'ID', 'CHROM', 'POS']
    for col in required_cols:
        if col not in input_df.columns:
            return pd.DataFrame(columns=["Sample_Id", "SV_Status", "Site1_Hugo_Symbol", "Site1_Region_Number", "Site2_Hugo_Symbol", "Site2_Region_Number", "NCBI_Build", "Class", "Method", "Event_Info", "Annotation", "DNA_Support", "RNA_Support", "Tumor_Read_Count", "Site1_Chromosome", "Site1_Position", "Site2_Chromosome", "Site2_Position"])
    
    fusion_df = input_df.copy()
    
    # Check if INFO column exists and has string data before filtering
    if 'INFO' not in fusion_df.columns or fusion_df['INFO'].empty:
        return pd.DataFrame(columns=["Sample_Id", "SV_Status", "Site1_Hugo_Symbol", "Site1_Region_Number", "Site2_Hugo_Symbol", "Site2_Region_Number", "NCBI_Build", "Class", "Method", "Event_Info", "Annotation", "DNA_Support", "RNA_Support", "Tumor_Read_Count", "Site1_Chromosome", "Site1_Position", "Site2_Chromosome", "Site2_Position"])
    
    fusion_df = fusion_df[fusion_df['INFO'].astype(str).str.contains("SVTYPE=Fusion", na=False)]
    
    if len(fusion_df) == 0:
        return pd.DataFrame(columns=["Sample_Id", "SV_Status", "Site1_Hugo_Symbol", "Site1_Region_Number", "Site2_Hugo_Symbol", "Site2_Region_Number", "NCBI_Build", "Class", "Method", "Event_Info", "Annotation", "DNA_Support", "RNA_Support", "Tumor_Read_Count", "Site1_Chromosome", "Site1_Position", "Site2_Chromosome", "Site2_Position"])
    
    # output array
    op = []
    
    # get uniq fusion ID by taking ID and removing _1 or _2 at the end 
    fusion_df.loc[:, 'FUSION_ID'] = fusion_df['ID'].str.split("_").str[0]
    
    # get data  from INFO column; gene names are in the format GENE_NAME=<NAME>;
    fusion_df.loc[:, 'GENE'] = fusion_df['INFO'].astype(str).str.extract(r'GENE_NAME=([^;]+)')
    fusion_df.loc[:, 'EXON'] = fusion_df['INFO'].astype(str).str.extract(r'EXON_NUM=([^;]+)')
    fusion_df.loc[:, 'Annotation'] = fusion_df['INFO'].astype(str).str.extract(r'ANNOTATION=([^;]+)')
    fusion_df.loc[:, 'Tumor_Read_Count'] = fusion_df['INFO'].astype(str).str.extract(r'READ_COUNT=([^;]+)')
    
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
    
    tumor_cols = tumor_fusion.columns.tolist()
    
    # remove from tumor fusion any fusions that are also in normal fusion based on Site1_Hugo_Symbol, Site2_Hugo_Symbol, Site1_Region_Number, and Site2_Region_Number
    merged_fusion = tumor_fusion.merge(normal_fusion, on=["Site1_Hugo_Symbol", "Site2_Hugo_Symbol", "Site1_Region_Number", "Site2_Region_Number"], how="left", indicator=True, suffixes=("", "_normal"))
    merged_fusion.loc[merged_fusion["_merge"] == "both", "SV_Status"] = "GERMLINE"
    
    # remove GERMLINE fusions that are in tumor fusion from tumor fusion
    merged_fusion = merged_fusion[merged_fusion["SV_Status"] != "GERMLINE"]
    
    # remove columns from normal fusion that are not needed for output
    merged_fusion = merged_fusion[tumor_cols]
    
    logger.info(f"Somatic fusion after removing germline fusions for {tumor_sample_id}: {len(merged_fusion)}")
    
    return merged_fusion

# task for cnv file prep
@task(name="cnv_file_prep", log_prints=True)
def cnv_file_prep(input_df: pd.DataFrame, sample_id: str) -> pd.DataFrame:
    """Read in CNV file and prep pertinent columns for annotation and merging to maf"""
    
    # Check if required columns exist
    required_cols = ['ALT', 'FILTER', 'INFO', 'ID', 'CHROM', 'POS']
    for col in required_cols:
        if col not in input_df.columns:
            return pd.DataFrame(columns=["Sample_Id", "Patient_Id", "Hugo_Symbol", "chromosome", "start", "end", "num.mark", "seg.mean", "copy_number"])
    
    cnv_df = input_df.copy()
    cnv_df = cnv_df[(cnv_df.ALT == "<CNV>") & (cnv_df.INFO.astype(str).str.contains('Amplification', na=False))]
    
    if len(cnv_df) == 0:
        return pd.DataFrame(columns=["Sample_Id", "Patient_Id", "Hugo_Symbol", "chromosome", "start", "end", "num.mark", "seg.mean", "copy_number"])
    
    # get data  from INFO column
    cnv_df.loc[:, 'Num_Probes'] = cnv_df['INFO'].astype(str).str.extract(r'NUMTILES=([^;]+)')
    cnv_df.loc[:, 'End'] = cnv_df['INFO'].astype(str).str.extract(r'END=([^;]+)')
    cnv_df.loc[:, 'cn'] = cnv_df['INFO'].astype(str).str.extract(r'RAW_CN=([^;]+)')
    cnv_df.loc[:, 'log2'] = cnv_df['cn'].apply(lambda x: copy_number_to_log2(float(x)))
    
    # op array
    op = []
    
    for _, row in cnv_df.iterrows():
        temp_dict = {
            "ID": sample_id,
            "Patient_Id": sample_id.split("_")[0],
            "Hugo_Symbol": row["ID"],
            "chrom": row["CHROM"],
            "loc.start": row["POS"],
            "loc.end": row["End"],
            "num.mark": row["Num_Probes"],
            "seg.mean": row["log2"], ##
            "copy_number": row["cn"], ##
        }
        op.append(temp_dict)
    
    return pd.DataFrame(op)

# task to format cnv segment
@task(name="cnv_segment_file_prep", log_prints=True)
def cnv_segment_file_prep(input_df: pd.DataFrame) -> pd.DataFrame:
    """Read in CNV segment file and prep pertinent columns for annotation and merging to maf"""
    
    # subset to required columns
    required_cols = ['ID', 'chrom', 'loc.start', 'loc.end', 'num.mark', 'seg.mean']
    cna_segment_df = input_df[required_cols].copy()
    
    # make sure loc.start and loc.end are integers
    cna_segment_df['loc.start'] = cna_segment_df['loc.start'].astype(int)
    cna_segment_df['loc.end'] = cna_segment_df['loc.end'].astype(int)
    
    return cna_segment_df
    
# task to format cnv discrete
@task(name="cnv_discrete_file_prep", log_prints=True)
def cnv_discrete_file_prep(input_df: pd.DataFrame) -> pd.DataFrame:
    """Read in CNV discrete file and prep pertinent columns for annotation and merging to maf"""
    
    # subset to required columns
    required_cols = ['ID', 'Hugo_Symbol', 'seg.mean']
    
    cna_discrete_df = input_df[required_cols].copy()
    
    # perform binning of values with gistic_like_calls function
    cna_discrete_df.loc[:, 'discrete_copy_number'] = cna_discrete_df['seg.mean'].apply(lambda x: gistic_like_calls(x))
    
    cna_discrete_pivot = cna_discrete_df.pivot(
        index='Hugo_Symbol',
        columns='ID',
        values='discrete_copy_number'
    ).reset_index().fillna(0)
    
    # convert all values to int
    for col in cna_discrete_pivot.columns[1:]:
        cna_discrete_pivot[col] = cna_discrete_pivot[col].astype(int)
    
    return cna_discrete_pivot

# task to format cnv log2 continuous
@task(name="cnv_log2_continuous_file_prep", log_prints=True)
def cnv_log2_continuous_file_prep(input_df: pd.DataFrame) -> pd.DataFrame:
    """Read in CNV discrete file and prep pertinent columns for annotation and merging to maf"""
    
    # subset to required columns
    required_cols = ['ID', 'Hugo_Symbol', 'seg.mean']
    
    cna_log2_df = input_df[required_cols].copy()
    
    cna_log2_pivot = cna_log2_df.pivot(
        index='Hugo_Symbol',
        columns='ID',
        values='seg.mean'
    ).reset_index().fillna('NA')
    
    return cna_log2_pivot

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
    filtered_cnv = tumor_cnv[~tumor_cnv["Hugo_Symbol"].isin(normal_cnv["Hugo_Symbol"])]
    
    print(f"Somatic CNV after removing germline CNVs for {tumor_sample_id}: {len(filtered_cnv)}")
    logger.info(f"Somatic CNV after removing germline CNVs for {tumor_sample_id}: {len(filtered_cnv)}")
    
    return filtered_cnv

# snv flow
@flow(name="snv_flow", log_prints=True)
def snv_flow(tumor_vcf: str, tumor_sample_id: str, normal_vcf: str, normal_sample_id: str, output_path :str, logger) -> pd.DataFrame:
    """Flow to process SNV data from clinical VCFs for pedmatch"""
    
    # dir for intermediate files
    intermediate_dir = os.path.join(output_path, f"{tumor_sample_id}_intermediate_files")
    
    # create dir for intermediate files
    os.makedirs(intermediate_dir, exist_ok=True)
    
    # rename barcodes in vcf files
    print(f"Renaming barcodes in VCF files for {tumor_sample_id} and {normal_sample_id}")
    command = [f"sed -i 's/{tumor_sample_id.split('_')[0]}/{tumor_sample_id}/g' {tumor_vcf} && sed -i 's/{normal_sample_id.split('_')[0]}/{normal_sample_id}/g' {normal_vcf}"]
    shell_op = ShellOperation(commands=command)
    shell_op.run()
    
    # preserve FILTER as FT in tumor and normal vcf files
    print(f"Preserving FILTER as FT in VCF files for {tumor_sample_id} and {normal_sample_id}")
    def preserve_filter(vcf, intermediate_dir):
        file_lines = open(vcf).readlines()
        # insert into lines header line at line 100 for new FT field in FORMAT column and new value in sample column
        filter_header = '##FORMAT=<ID=FT,Number=1,Type=String,Description="Filter status of the variant">\n'
        file_lines.insert(100, filter_header)
        #open(os.path.join(intermediate_dir, os.path.basename(vcf).replace(".vcf", ".withFT.vcf")), "w").write(filter_header)
        for line in file_lines:
            if line.startswith("#"):
                open(os.path.join(intermediate_dir, os.path.basename(vcf).replace(".vcf", ".withFT.vcf")), "a").write(line)
            else:
                cols = line.strip().split("\t")
                filter_col = cols[6]
                format_col = cols[8]
                sample_col = cols[9]
                format_col += ":FT"
                sample_col += f":{filter_col}"
                cols[8] = format_col
                cols[9] = sample_col
                new_line = "\t".join(cols) + "\n"
                open(os.path.join(intermediate_dir, os.path.basename(vcf).replace(".vcf", ".withFT.vcf")), "a").write(new_line)
    
    preserve_filter(tumor_vcf, intermediate_dir)
    preserve_filter(normal_vcf, intermediate_dir)
    
    # sort and tabix index the files
    print(f"Sorting and indexing VCF files for {tumor_sample_id} and {normal_sample_id}")
    command = [f"bcftools sort -O z -o {intermediate_dir}/{tumor_sample_id}_tumor.sorted.vcf.gz {intermediate_dir}/{os.path.basename(tumor_vcf).replace('.vcf', '.withFT.vcf')} && bcftools sort -O z -o {intermediate_dir}/{normal_sample_id}_normal.sorted.vcf.gz {intermediate_dir}/{os.path.basename(normal_vcf).replace('.vcf', '.withFT.vcf')} && tabix -p vcf {intermediate_dir}/{tumor_sample_id}_tumor.sorted.vcf.gz && tabix -p vcf {intermediate_dir}/{normal_sample_id}_normal.sorted.vcf.gz"]
    shell_op = ShellOperation(commands=command)
    shell_op.run()   
    
    # merge tumor and normal vcf files using bcftools merge
    print(f"Merging tumor and normal VCF files for {tumor_sample_id} and {normal_sample_id}")
    command = [f"bcftools merge -m id -O z -o {intermediate_dir}/{tumor_sample_id}_merged.vcf.gz {intermediate_dir}/{tumor_sample_id}_tumor.sorted.vcf.gz {intermediate_dir}/{normal_sample_id}_normal.sorted.vcf.gz"]
    shell_op = ShellOperation(commands=command)
    shell_op.run()
    
    # split multiallelic sites in merged vcf file using bcftools norm
    print(f"Splitting multiallelic sites in merged VCF file for {tumor_sample_id} and {normal_sample_id}")
    command = [f"bcftools norm -m -any -O z -o {intermediate_dir}/{tumor_sample_id}_merged.split.vcf.gz {intermediate_dir}/{tumor_sample_id}_merged.vcf.gz && tabix -p vcf {intermediate_dir}/{tumor_sample_id}_merged.split.vcf.gz"]
    shell_op = ShellOperation(commands=command)
    shell_op.run()
    
    # apply somatic filter
    print(f"Applying somatic filter to merged VCF file for {tumor_sample_id} and {normal_sample_id}")
    command = [f"bcftools view -i 'FORMAT/DP[0] >= 20 && FORMAT/DP[1] >= 15 && FORMAT/AF[0:0] >= 0.05 && FORMAT/AF[1:0] <= 0.02' {intermediate_dir}/{tumor_sample_id}_merged.split.vcf.gz -O z -o {intermediate_dir}/{tumor_sample_id}_somatic.vcf.gz"]
    shell_op = ShellOperation(commands=command)
    shell_op.run()
    
    # func to extract GT
    def gt_extract(gt_str):
        if pd.isna(gt_str):
            return "NA"
        else:
            return gt_str.split(":")[0]
        
    # func to extract FILTER
    def filter_extract(filter_str):
        if pd.isna(filter_str):
            return "NA"
        else:
            return filter_str.split(":")[-1]
            
    def af_extract(af_str):
        if pd.isna(af_str):
            return "NA"
        else:
            return af_str.split(":")[8]
    
    def dp_extract(dp_str):
        if pd.isna(dp_str):
            return "NA"
        else:
            return dp_str.split(":")[2]
    
    def fao_extract(dp_str):
        if pd.isna(dp_str):
            return "NA"
        else:
            return dp_str.split(":")[7]
    
    def fro_extract(dp_str):
        if pd.isna(dp_str):
            return "NA"
        else:
            return dp_str.split(":")[5]
        
    # read in somatic vcf file with pandas, filter and return dataframe
    print(f"Reading in somatic VCF file for {tumor_sample_id} and {normal_sample_id}")
    somatic_vcf_df = pd.read_csv(f"{intermediate_dir}/{tumor_sample_id}_somatic.vcf.gz", sep="\t", comment="#", names=VCF_HEADER_COLS+[tumor_sample_id, normal_sample_id], low_memory=False)
    
    # tumor GT
    somatic_vcf_df.loc[:, "tumor_gt"] = somatic_vcf_df[tumor_sample_id].apply(lambda x: gt_extract(x))
    # normal GT
    somatic_vcf_df.loc[:, "normal_gt"] = somatic_vcf_df[normal_sample_id].apply(lambda x: gt_extract(x))
    # tumor FILTER
    somatic_vcf_df.loc[:, "tumor_filter"] = somatic_vcf_df[tumor_sample_id].apply(lambda x: filter_extract(x))
    # normal FILTER
    somatic_vcf_df.loc[:, "normal_filter"] = somatic_vcf_df[normal_sample_id].apply(lambda x: filter_extract(x))
    # tumor AF
    somatic_vcf_df.loc[:, "tumor_af"] = somatic_vcf_df[tumor_sample_id].apply(lambda x: af_extract(x))
    # normal AF
    somatic_vcf_df.loc[:, "normal_af"] = somatic_vcf_df[normal_sample_id].apply(lambda x: af_extract(x))
    # tumor DP
    somatic_vcf_df.loc[:, "t_depth"] = somatic_vcf_df[tumor_sample_id].apply(lambda x: dp_extract(x))
    # normal DP
    somatic_vcf_df.loc[:, "n_depth"] = somatic_vcf_df[normal_sample_id].apply(lambda x: dp_extract(x))
    # t_alt_count
    somatic_vcf_df.loc[:, "t_alt_count"] = somatic_vcf_df[tumor_sample_id].apply(lambda x: fao_extract(x))
    # t_ref_count
    somatic_vcf_df.loc[:, "t_ref_count"] = somatic_vcf_df[tumor_sample_id].apply(lambda x: fro_extract(x))
    
    #filter somatic filters 
    somatic_vcf_df = somatic_vcf_df[(somatic_vcf_df['tumor_filter'] == 'PASS') & ~(somatic_vcf_df.INFO.str.contains('SVTYPE')) & (somatic_vcf_df["tumor_gt"] != ("0/0")) & (somatic_vcf_df["normal_gt"] != somatic_vcf_df["tumor_gt"])]
    somatic_vcf_name = f"{tumor_sample_id}_somatic_snvs.vcf"
    # save VCF for processing
    somatic_vcf_df.to_csv(os.path.join(intermediate_dir, somatic_vcf_name), sep="\t", index=False)    
    
    #save backup for filter details
    somatic_vcf_df.to_csv(os.path.join(intermediate_dir, f"{tumor_sample_id}_somatic_snvs_backup.vcf"), sep="\t", index=False)    
    
    logger.info(f"Somatic SNVs after filtering for {tumor_sample_id}: {len(somatic_vcf_df)}")
    print(f"Somatic SNVs after filtering for {tumor_sample_id}: {len(somatic_vcf_df)}")
    if len(somatic_vcf_df) == 0: #no SNVs found in tumor sample, return empty df with correct columns
        return None
        
    # write allele fractoion data to separate file for use in annotation
    af_df = somatic_vcf_df[["CHROM", "POS", "t_alt_count", "t_ref_count"]].copy()
    af_df.columns = ["Chromosome", "Start_Position", "t_alt_count", "t_ref_count"]
    af_df['Tumor_Sample_Barcode'] = tumor_sample_id
    af_df.to_csv(os.path.join(intermediate_dir, f"{tumor_sample_id}_af.tsv"), sep="\t", index=False)    
    
    return somatic_vcf_name


@task(name="concat_maf_check", log_prints=True)
def concat_maf_check(output_path: str, concatenated_maf_name: str, line_count_filename: str, manifest_df: pd.DataFrame, dt: str, logger, runner_logger) -> None:
    """Check concatenated MAF file line counts

    Args:
        output_path (str): Path to output directory
        concatenated_maf_name (str): Name of concatenated MAF file
        line_count_filename (str): Name of line count file
        manifest_df (pd.DataFrame): Dataframe of manifest file
        dt (str): Date string
        logger: Logger object
        runner_logger: Runner logger object
    """
    
    # samples to rerun init
    samples_to_rerun = []
    
    # read in line count file
    line_counts = pd.read_csv(os.path.join(output_path, line_count_filename), sep='\s+', header=None, names=['line_count', 'file_name'])
    
    # add in file_name for mapping to line_counts
    line_counts['file_name'] = line_counts['file_name'].apply(lambda x: os.path.basename(x))
    
    # remove count of header line from line counts
    line_counts['line_count'] = line_counts['line_count'].astype(int) - 1
    
    # map samples to line counts by transformed file name
    manifest_df = manifest_df[manifest_df.sample_type == 'tissue'].copy()
    manifest_df['file_name'] = manifest_df['s3_url'].apply(lambda x: os.path.basename(x).split("_")[0]+"_vcf_tissue_somatic_snvs.vcf")
    merged_df = pd.merge(manifest_df, line_counts, on='file_name', how='left')
    
    # read in concatenated MAF file and get line count by Tumor_Sample_Barcode
    concat_maf = pd.read_csv(os.path.join(output_path, concatenated_maf_name), sep='\t', comment='#', low_memory=False)
    concat_line_counts = concat_maf['Tumor_Sample_Barcode'].value_counts().reset_index()
    concat_line_counts.columns = ['Tumor_Sample_Barcode', 'line_count']
    
    # rename columns for merging
    merged_df = pd.merge(merged_df, concat_line_counts, left_on='sample_id', right_on='Tumor_Sample_Barcode', how='left', suffixes=('_individual', '_concat'))
    
    # compare line counts
    discrepancies = merged_df[merged_df['line_count_individual'] != merged_df['line_count_concat']]
    if not discrepancies.empty:
        runner_logger.error("Discrepancies found in line counts between individual MAFs and concatenated MAF:")
        logger.error("Discrepancies found in line counts between individual MAFs and concatenated MAF:")
        for _, row in discrepancies.iterrows():
            runner_logger.error(f"Sample: {row['sample_id']}, Individual line count: {row['line_count_individual']}, Concatenated line count: {row['line_count_concat']}")
            logger.error(f"Sample: {row['sample_id']}, Individual line count: {row['line_count_individual']}, Concatenated line count: {row['line_count_concat']}")
            samples_to_rerun.append(row['sample_id'])
    
    # check for FAILED annotations in col Annotation_Status
    fail_check_df = concat_maf[(concat_maf.Annotation_Status == 'FAILED')].groupby('Tumor_Sample_Barcode').size()
    
    if not fail_check_df.empty:
        runner_logger.error("Samples with FAILED annotations found in concatenated MAF:")
        logger.error("Samples with FAILED annotations found in concatenated MAF:")
        for sample_id in fail_check_df.index:
            runner_logger.error(f"Sample: {sample_id}, Failed annotations: {fail_check_df[sample_id]}")
            logger.error(f"Sample: {sample_id}, Failed annotations: {fail_check_df[sample_id]}")
            samples_to_rerun.append(sample_id)
            
    # check for misformatted variants with null/missing Tumor_Sample_Barcode
    misformatted_df = concat_maf[concat_maf['Tumor_Sample_Barcode'].isnull() | (concat_maf['Tumor_Sample_Barcode'] == '')]
    if not misformatted_df.empty:
        runner_logger.error("Misformatted variants with null/missing Tumor_Sample_Barcode found in concatenated MAF:")
        logger.error("Misformatted variants with null/missing Tumor_Sample_Barcode found in concatenated MAF:")
        for _, row in misformatted_df.iterrows():
            runner_logger.error(f"Variant: {row['Chromosome']}:{row['Start_Position']} {row['Reference_Allele']}>{row['Tumor_Seq_Allele1']}")
            logger.error(f"Variant: {row['Chromosome']}:{row['Start_Position']} {row['Reference_Allele']}>{row['Tumor_Seq_Allele1']}")

    # save new concat MAF without samples identified in samples_to_rerun OR Tumor_Sample_Barcode null/missing
    if samples_to_rerun or not misformatted_df.empty:
        cleaned_concat_maf = concat_maf[~concat_maf['Tumor_Sample_Barcode'].isin(samples_to_rerun)]
        cleaned_concat_maf = cleaned_concat_maf[~(cleaned_concat_maf['Tumor_Sample_Barcode'].isnull() | (cleaned_concat_maf['Tumor_Sample_Barcode'] == ''))]
        cleaned_concat_maf.to_csv(os.path.join(output_path, concatenated_maf_name.replace('.maf', '_cleaned.maf')), sep='\t', index=False)
        runner_logger.info(f"Cleaned concatenated MAF file saved as {concatenated_maf_name.replace('.maf', '_cleaned.maf')}")
        logger.info(f"Cleaned concatenated MAF file saved as {concatenated_maf_name.replace('.maf', '_cleaned.maf')}")
        
        #create new manifest df for samples to rerun
        rerun_manifest_df = manifest_df[manifest_df['sample_id'].isin(samples_to_rerun)]
        rerun_manifest_df.to_csv(os.path.join(output_path, f"vcf_annotation_rerun_manifest_{dt}.csv"), index=False)
        runner_logger.info(f"Rerun manifest file saved as vcf_annotation_rerun_manifest_{dt}.csv")
        logger.info(f"Rerun manifest file saved as vcf_annotation_rerun_manifest_{dt}.csv")
    else:
        runner_logger.info("No discrepancies found in concatenated MAF file.")
        logger.info("No discrepancies found in concatenated MAF file.")

    return None

# patient task - converted from flow to task for concurrent mapping
@task(name="pt_paired_vcf_task", log_prints=True, retries=2, retry_delay_seconds=30, task_run_name="process-{tumor_sample_id}")
def pt_paired_vcf_task(tumor_vcf, tumor_sample_id, normal_vcf, normal_sample_id, output_path, logger):
    """Process a single patient's tumor/normal VCF pair concurrently"""
    # prep files
    tumor_vcf_df = clin_vcf_file_prep(tumor_vcf, tumor_sample_id)
    normal_vcf_df = clin_vcf_file_prep(normal_vcf, normal_sample_id)
    
    # process fusion data
    fusion_results = fusion_flow(tumor_vcf_df, tumor_sample_id, normal_vcf_df, normal_sample_id, logger)
    cnv_results = cnv_flow(tumor_vcf_df, tumor_sample_id, normal_vcf_df, normal_sample_id, logger)
    somatic_vcf_name = snv_flow(tumor_vcf, tumor_sample_id, normal_vcf, normal_sample_id, output_path, logger)
    
    return fusion_results, cnv_results, somatic_vcf_name

@task(name="prepare_patient_data", log_prints=True)
def prepare_patient_data(batch_df: pd.DataFrame, logger, runner_logger):
    """Prepare patient data for concurrent processing"""
    patient_data = []
    
    # iterate thru tumor normal pairs to prepare data
    for group_name, group_df in batch_df.groupby("participant_id"):
        tumor_df = group_df[group_df["sample_type"] == "tissue"]
        normal_df = group_df[group_df["sample_type"] == "blood"]
        
        if len(tumor_df) != 1 or len(normal_df) != 1:
            runner_logger.warning(f"Skipping participant {group_name} due to incorrect number of tumor or normal samples")
            logger.warning(f"Participant {group_name} tumor samples: {len(tumor_df)}, normal samples: {len(normal_df)}")
            continue
            
        patient_data.append({
            'participant_id': group_name,
            'tumor_vcf': tumor_df.iloc[0]["file_name"],
            'tumor_sample_id': tumor_df.iloc[0]["sample_id"],
            'normal_vcf': normal_df.iloc[0]["file_name"], 
            'normal_sample_id': normal_df.iloc[0]["sample_id"]
        })
    
    return patient_data

@task(name="process_patient_results", log_prints=True)
def process_patient_results(results, patient_data, output_path, logger, runner_logger):
    """Process results from concurrent patient processing"""
    fusion_op = []
    cnv_op = []
    somatic_vcf_op = []
    
    for i, result in enumerate(results):
        if result is None:  # Handle failed tasks
            runner_logger.warning(f"Skipping failed patient processing task {i}")
            continue
            
        fusion_results, cnv_results, somatic_vcf_name = result
        patient = patient_data[i]
        
        # add to output arrays
        fusion_op.append(fusion_results)
        cnv_op.append(cnv_results)
        if somatic_vcf_name is not None:
            somatic_vcf_op.append({
                'vcf_file': somatic_vcf_name, 
                'sample_barcode': patient['tumor_sample_id'], 
                'download_dir': os.path.join(output_path, f"{patient['tumor_sample_id']}_intermediate_files"), 
                'output_dir': output_path, 
                'reference_genome': "GRCh37"
            })
    
    return fusion_op, cnv_op, somatic_vcf_op

# batch flow with concurrent processing
@flow(name="batch_process", log_prints=True, task_runner=ConcurrentTaskRunner(max_workers=4))
def batch_process(batch_df: pd.DataFrame, output_path, logger, runner_logger) -> None:
    """Process patients concurrently using Prefect task mapping"""
    
    # Prepare patient data for concurrent processing
    patient_data = prepare_patient_data(batch_df, logger, runner_logger)
    
    if not patient_data:
        runner_logger.warning("No valid patient data to process")
        return [], [], []
    
    runner_logger.info(f"Processing {len(patient_data)} patients concurrently with max 4 workers")
    
    # Process patients concurrently using task mapping
    patient_results = pt_paired_vcf_task.map(
        [p['tumor_vcf'] for p in patient_data],
        [p['tumor_sample_id'] for p in patient_data], 
        [p['normal_vcf'] for p in patient_data],
        [p['normal_sample_id'] for p in patient_data],
        [output_path] * len(patient_data),  # Replicate static parameter
        [logger] * len(patient_data)  # Replicate static parameter
    )
    
    # Process results
    return process_patient_results(patient_results, patient_data, output_path, logger, runner_logger)

# main flow:
@flow(name="pedmatch_clinical_vcf_flow", log_prints=True, task_runner=ConcurrentTaskRunner(max_workers=2))
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
    
    # make output directory on mounted drive
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
    batch_size = 20 # temp for testing with 20 files; change to 500 for full run
    #for i in range(0, len(manifest_df), batch_size):
    for i in range(0, 20, batch_size): # temp for testing with 500 files
        batch_df = manifest_df.iloc[i:i+batch_size]
        runner_logger.info(f"Downloading batch {i//batch_size + 1} of {len(manifest_df.head(20))//batch_size + 1}")
        download_cnv(batch_df, logger)
    
    fusion_concat_results= []
    cnv_concat_results = []
    snv_int_results = []
    
    # add batch loop here
    for i in range(0, len(manifest_df.head(20)), batch_size):
        batch_df = manifest_df.iloc[i:i+batch_size]
        runner_logger.info(f"Processing batch {i//batch_size + 1} of {len(manifest_df.head(20))//batch_size + 1}")
        fusion_batch_op, cnv_batch_op, snv_batch_op = batch_process(batch_df, output_path, logger, runner_logger)
        fusion_concat_results.extend(fusion_batch_op)
        cnv_concat_results.extend(cnv_batch_op)
        snv_int_results.extend(snv_batch_op)
    
    # save output files
    fusion_output_path = os.path.join(output_path, "data_sv.txt")
    cnv_output_path = os.path.join(output_path, "cnv_int_results.txt")
    
    pd.concat(fusion_concat_results).to_csv(fusion_output_path, sep="\t", index=False)
    cnv_concat_results_df = pd.concat(cnv_concat_results)
    cnv_concat_results_df.to_csv(cnv_output_path, sep="\t", index=False)
    
    # post-hoc processing of cnvs into cBio format files
    cna_seg = cnv_segment_file_prep(cnv_concat_results_df)
    cna_discrete = cnv_discrete_file_prep(cnv_concat_results_df)
    cna_log2_continuous = cnv_log2_continuous_file_prep(cnv_concat_results_df)
    
    # save cna files
    cna_seg.to_csv(os.path.join(output_path, "data_cna_hg19.seg.txt"), sep="\t", index=False)
    cna_discrete.to_csv(os.path.join(output_path, "data_cna.txt"), sep="\t", index=False)
    cna_log2_continuous.to_csv(os.path.join(output_path, "data_log2_cna.txt"), sep="\t", index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
    
    # genome nexus annotation of snvs
    install_nexus()
    
    # annotate VCFs concurrently
    @flow(name="snv_annotation", log_prints=True, task_runner=ConcurrentTaskRunner(max_workers=10))
    def snv_annotation_flow(snv_dict_list: list[dict], logger):
        """Annotate SNVs using concurrent processing"""
        if not snv_dict_list:
            return []
        runner_logger.info(f"Annotating {len(snv_dict_list)} VCF files concurrently with max 10 workers")
        return annotator.map(
            snv_dict_list, 
            [logger] * len(snv_dict_list)  # Replicate logger for each task
        )
    
    # Process all SNV annotations at once instead of batching
    if snv_int_results:
        snv_annotation_flow(snv_int_results, logger)
        
    # add VAF back to MAFs
    maf_files = [f for f in os.listdir(output_path) if f.endswith(".maf")]
    @task(name="add_vaf", log_prints=True, tags=["vcf_anno_task-tag"], retries=3, retry_delay_seconds=10)
    def add_vaf(maf_file, output_path):
        maf_df = pd.read_csv(os.path.join(output_path, maf_file), sep="\t", comment="#", low_memory=False)
        sample_id = maf_df['Tumor_Sample_Barcode'].iloc[0]
        af_file = os.path.join(output_path, f"{sample_id}_intermediate_files", f"{sample_id}_af.tsv")
        af_df = pd.read_csv(af_file, sep="\t")[["Chromosome", "Start_Position", "t_alt_count", "t_ref_count"]]
        # Chromosome and Start_postion astype(int)
        maf_df['Start_Position'] = maf_df['Start_Position'].astype(int)
        af_df['Start_Position'] = af_df['Start_Position'].astype(int)
        maf_df['Chromosome'] = maf_df['Chromosome'].replace('chr', '').astype(int)
        af_df['Chromosome'] = af_df['Chromosome'].replace('chr', '').astype(int)
        maf_df = maf_df.merge(af_df, left_on=['Chromosome', 'Start_Position'], right_on=['Chromosome', 'Start_Position'], how='left')
        maf_df.to_csv(os.path.join(output_path, maf_file), sep="\t", index=False)
    
    @flow(name="add_vaf_flow", log_prints=True, task_runner=ConcurrentTaskRunner(max_workers=8))
    def add_vaf_flow(maf_files: list, output_path):
        """Add VAF data to MAF files concurrently"""
        if not maf_files:
            return
        runner_logger.info(f"Adding VAF to {len(maf_files)} MAF files concurrently with max 8 workers")
        return add_vaf.map(
            maf_files, 
            [output_path] * len(maf_files)  # Replicate output_path for each task
        )
        
    # Process all MAF files at once instead of batching
    if maf_files:
        add_vaf_flow(maf_files, output_path)
    
    # concat MAFs and save
    concat_mafs(maf_files, output_path, "data_mutations.txt", dt, logger, runner_logger)
    
    # concat merged MAF line check per sample
    concat_maf_check(output_path, "data_mutations.txt", f"vcf_annotated_line_counts_{dt}.txt", manifest_df.head(20), dt, logger, runner_logger)
    
    # upload dir to s3
    upload_folder_to_s3(
            local_folder=output_path,
            bucket=bucket,
            destination=output_dir,
            sub_folder=""
        )