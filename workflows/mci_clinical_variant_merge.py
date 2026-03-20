import os, sys
import pandas as pd
from time import sleep
import requests
from prefect import flow, task
from src.utils import get_time, file_dl, upload_folder_to_s3, get_run_logger
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
import shutil
from typing import Literal
import openpyxl

@task(name="clin_file_prep", description="Prepares clinical variant file for annotation by filtering to relevant variants and parsing out necessary columns")
def clin_file_prep(clin_file_path: str, maf_samples: list, reference_genome: str) -> pd.DataFrame:
    """Prepares clinical variant file for annotation by filtering to relevant variants and parsing out necessary columns
    
    Args:
        clin_file_path (str): Local path to clinical variant file to be prepared for annotation
        maf_samples (list): List of sample IDs from the MAF file to filter clinical variants
        reference_genome (str): Reference genome to use for annotation (e.g., "GRCh38" or "GRCh37")
    Returns:
        pd.DataFrame: Dataframe of clinical variants filtered to those relevant for annotation and with necessary columns parsed out
    """
    # annotate clinical variants with Genome Nexus API
    clin_muts = pd.read_excel(clin_file_path, sheet_name="genetic_analysis", engine="openpyxl")
    
    # filter clin muts to those with samples in maf 
    clin_muts = clin_muts[clin_muts["sample.sample_id"].isin(maf_samples)]
    
    # filter clin muts to Somatic variants that are Present only
    clin_muts = clin_muts[(clin_muts["genomic_source_category"]=="Somatic") & (clin_muts["status"]=="Present")]
    
    # filter to SNVs only for annotation with Genome Nexus API
    # NOTE: may need to eventually update to using different variable to filter down to SNVs
    clin_muts = clin_muts[(clin_muts["test"]=="Somatic Disease/Germline Comparator Exome") & (clin_muts["reported_significance_system"]=="AMP/ASCO/CAP")]
    
    # parse out relevant cols
    cols = ["sample.sample_id", "gene_symbol", "transcript", "chromosome", "hgvs_genome", "hgvs_coding", "hgvs_protein", "reported_significance_system", "reported_significance"]
    clin_muts = clin_muts[cols]
    
    # remove chr prefix for genome nexus query if needed
    clin_muts["chromosome"] = clin_muts["chromosome"].apply(lambda x : x.replace("chr", "") if str(x).startswith("chr") else x)
    
    # add reference genome 
    clin_muts["reference_genome"] = reference_genome
    
    # create query col for Genome Nexus API annotation
    clin_muts["query"] = clin_muts.apply(lambda row: f"{row['chromosome']}:{row['hgvs_genome']}", axis=1)
    
    return clin_muts

@task(name="fetch_variant", description="Fetches variant annotation from Genome Nexus API for a given variant")
def fetch_variant(row, reference_genome, retries=3) -> dict:
    for attempt in range(retries):
        try:
            if reference_genome == 'GRCh38':
                r = requests.get(
                    f"https://grch38.genomenexus.org/annotation/{row.query}?fields=hotspots,annotation_summary,my_variant_info,clinvar,signal,mutation_assessor",
                    timeout=10
                )
            elif reference_genome == 'GRCh37':
                r = requests.get(
                    f"https://www.genomenexus.org/annotation/{row.query}?fields=hotspots,annotation_summary,my_variant_info,clinvar,signal,mutation_assessor",
                    timeout=10
                )
            data = r.json()

            ann = data.get('annotation_summary', {})
            loc = ann.get('genomicLocation', {})

            result = {
                "start": loc.get('start'),
                "end": loc.get('end'),
                "variant_type": ann.get('variantType'),
                "reference_allele": loc.get('referenceAllele'),
                "variant_allele": loc.get('variantAllele'),
                "hgvs_short": None,
                "variant_classification": None
            }

            for cons in ann.get('transcriptConsequences', []):
                if row.hgvs_protein != 'Not Reported':
                    hgvsp = cons.get('hgvsp')
                    if hgvsp and (hgvsp == row.hgvs_protein or row.hgvs_protein in hgvsp):
                        result["hgvs_short"] = cons.get('hgvspShort')
                        result["variant_classification"] = cons.get('variantClassification')
                        break
                else:
                    if row.hgvs_coding in cons.get('hgvsc', ''):
                        result["hgvs_short"] = cons.get('hgvspShort')
                        result["variant_classification"] = cons.get('variantClassification')
                        break

            return result

        except Exception:
            if attempt < retries - 1:
                sleep(2)
            else:    
                return {
                    "start": None,
                    "end": None,
                    "variant_type": None,
                    "reference_allele": None,
                    "variant_allele": None,
                    "hgvs_short": None,
                    "variant_classification": None
                }

@flow(name="annotate_clinical_variants", description="Annotates clinical variants using Genome Nexus API")
def annotate_clinical_variants(clin_muts: pd.DataFrame, reference_genome) -> pd.DataFrame:
    """Annotates clinical variants using Genome Nexus API
    
    Args:
        clin_muts (pd.DataFrame): Dataframe of clinical variants to be annotated with necessary columns for annotation and query to Genome Nexus API
    Returns:
        pd.DataFrame: Dataframe of annotated clinical variants with annotation columns from Genome Nexus API added
    """
    
    # count clin muts
    starting_count = clin_muts.shape[0]
    
    op_df = []
    
    # query variants against genome nexus
    for batch in range(0, clin_muts.shape[0], 10):
        batch_rows = list(clin_muts.iloc[batch:batch+100].itertuples(index=False))
        with ThreadPoolExecutor(max_workers=10) as executor:
            batch_results = list(executor.map(fetch_variant, batch_rows, repeat(reference_genome)))
        op_df.extend(batch_results)
    result_df = pd.DataFrame(op_df)
    """rows = list(clin_muts.itertuples(index=False))
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(fetch_variant, rows, repeat(reference_genome)))
    result_df = pd.DataFrame(results)"""
    clin_muts = pd.concat([clin_muts.reset_index(drop=True), result_df], axis=1)
    
    # filter out any annotation failures
    clin_muts = clin_muts[clin_muts["start"].notna() & clin_muts["end"].notna() & clin_muts["variant_type"].notna() & clin_muts["reference_allele"].notna() & clin_muts["variant_allele"].notna()]
    
    # final count after anno
    ending_count = clin_muts.shape[0]
    
    # calc number not annotated
    not_anno = starting_count - ending_count
    
    # format results to be able to conact to maf and add annotation columns to maf
    cols = ["sample.sample_id", "gene_symbol", "transcript", "chromosome", "start", "end", "reference_allele", "variant_allele", "variant_classification", "hgvs_short", "reported_significance_system", "reported_significance", "reference_genome"]
    clin_muts = clin_muts[cols]
    
    # rename cols to match maf for merging
    clin_muts = clin_muts.rename(columns={'sample.sample_id' : 'Tumor_Sample_Barcode', 'transcript': 'RefSeq', 'reference_genome' : 'NCBI_Build','chromosome' : 'Chromosome','gene_symbol' : 'Hugo_Symbol','start' : 'Start_Position', 'end' : 'End_Position','variant_classification' : 'Variant_Classification','variant_type' : 'Variant_Type','reference_allele' : 'Reference_Allele','variant_allele' : 'Tumor_Seq_Allele2','hgvs_short' : 'HGVSp_Short', "reported_significance_system": "Reported.Significance System", "reported_significance": "Reported.Significance"})
    
    return clin_muts, not_anno

@task(name="merge_clinical_variants_to_maf", description="Merges annotated clinical variants to MAF file of raw variants, deduping with preference to clinical variants and adding annotation columns to maf")
def merge_clinical_variants_to_maf(maf_concat_path: str, anno_clin_muts: pd.DataFrame) -> pd.DataFrame:
    """Merges annotated clinical variants to MAF file of raw variants, deduping with preference to clinical variants and adding annotation columns to maf
    
    Args:
        anno_clin_muts (pd.DataFrame): Dataframe of annotated clinical variants with necessary columns for merging to maf
        maf_concat_path (str): S3 path to maf concat file of raw variants to be merged with annotated clinical variants
    Returns:
        pd.DataFrame: Dataframe of merged maf with annotated clinical variants, deduped with preference to clinical variants and with annotation columns added
    """
    # read in maf concat file
    maf_concat = pd.read_csv(maf_concat_path, sep="\t", comment="#", low_memory=False)
    
    # if Reported.Significance and Reported.Significance System cols don't exist in maf concat, add them with default value of "Not Reported"
    if "Reported.Significance System" not in maf_concat.columns:
        maf_concat["Reported.Significance System"] = "Not Reported"
    if "Reported.Significance" not in maf_concat.columns:
        maf_concat["Reported.Significance"] = "Not Reported"
        
    # count rows in raw maf concat
    unannotated_rows = maf_concat.shape[0]
    
    # count rows in annotated clin muts 
    clin_mut_rows = anno_clin_muts.shape[0]
    
    # concat annotated clin muts with maf concat, deduping with preference to clin muts and adding annotation cols to maf
    maf_concat = pd.concat([maf_concat, anno_clin_muts], ignore_index=True)#.drop_duplicates(subset=["Tumor_Sample_Barcode", "Hugo_Symbol", "Chromosome", "Start_Position", "End_Position", "Variant_Classification", "HGVSp_Short"], keep="first")
    
    # sort so that rows with Reported.Significance != Not Reported are at the top of the file for easier filtering for downstream analysis
    maf_concat_sorted = maf_concat.sort_values(by=["Reported.Significance System", "Reported.Significance"], key=lambda col: col.eq('Not Reported') | col.isna())
    
    # drop duplicates again after sorting to ensure that if there are any duplicates between clin muts and maf concat, the clin muts are kept
    subset_cols = [
        'Tumor_Sample_Barcode',
        'Hugo_Symbol',
        'Chromosome',
        'Start_Position',
        'End_Position',
        'Variant_Classification',
        'HGVSp_Short'
    ]
    
    maf_concat_sorted_deduped = maf_concat_sorted.drop_duplicates(subset=subset_cols, keep="first")
    
    # re-sort after deduping by Tumor_Sample_Barcode, Chromosome and Start_Position
    maf_concat_sorted_deduped = maf_concat_sorted_deduped.sort_values(by=["Tumor_Sample_Barcode", "Chromosome", "Start_Position"])
    
    # convert Entrez ID back to integer, fill NA with ''
    maf_concat_sorted_deduped['Entrez_Gene_Id'] = maf_concat_sorted_deduped['Entrez_Gene_Id'].astype('str').str.replace(".0", "").replace('nan', '')
    
    # count rows in new concat maf 
    concat_rows = maf_concat_sorted_deduped.shape[0]
    
    log_string = f"Number clinical variants: {clin_mut_rows}, \nNumber of variants in raw maf concat: {unannotated_rows} \nNumber of variants in merged maf: {concat_rows} \nNumber replaced: {clin_mut_rows-(concat_rows-unannotated_rows)} \nNumber NOT replaced: {concat_rows-unannotated_rows}\n"
    
    return maf_concat_sorted_deduped, log_string


DropDownChoices = Literal["GRCh37", "GRCh38"]

@flow(name="cbio-mci-vcf-clinical-variant-merge", description="Annotate clinical variants with Genome Nexus Annotation Pipeline and merge to megaMAF of raw variants")
def clin_anno_merge_flow(bucket: str, runner: str, clinical_variant_file_path: str, reference_genome: DropDownChoices, maf_concat_path: str):
    """"Flow to annotate clinical variants with Genome Nexus Annotation Pipeline and merge to megaMAF of raw variants
    
    Args:
        bucket (str): S3 bucket name for input and output files
        runner (str): Prefect runner to use for flow execution
        clinical_variant_file_path (str): S3 path to manifest file with variant data to be annotated and merged
        reference_genome (DropDownChoices): Reference genome build for annotation (e.g. "GRCh38")
        maf_concat_path (str): S3 path to maf concat file of raw variants to be merged with annotated clinical variants
    
    """
    runner_logger = get_run_logger()
    
    dt = get_time()
    
    # make output dir 
    output_path = os.path.join("/usr/local/data", "mci_clin_var_merge_"+dt)
    runner_logger.info(f"Output path: {output_path}")
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    # download clinical variant file and maf concat file
    file_dl(bucket, clinical_variant_file_path)
    file_dl(bucket, maf_concat_path)
    
    maf_concat_local_path = os.path.basename(maf_concat_path) 
    clin_var_local_path = os.path.basename(clinical_variant_file_path)
    
    runner_logger.info(f"Downloaded clinical variant file to {clin_var_local_path} and maf concat file to {maf_concat_local_path}")
    
    # get samples from maf concat file for filtering clinical variants to those with samples in maf
    maf_samples = pd.read_csv(maf_concat_local_path, sep="\t", comment="#", low_memory=False)["Tumor_Sample_Barcode"].unique().tolist()
    
    runner_logger.info("Prepping clinical mutations file")
    clin_muts = clin_file_prep(clin_var_local_path, maf_samples, reference_genome)
    
    runner_logger.info("Saving parsed clinical mutations file")
    clin_muts.to_csv(os.path.join(output_path, f"clin_muts_to_annotate_{dt}.tsv"), sep="\t", index=False) 
    
    runner_logger.info("Annotating clinical mutations")
    anno_clin_muts, not_anno = annotate_clinical_variants(clin_muts, reference_genome)
    
    runner_logger.info("Saving annotated clinical mutations file")
    anno_clin_muts.to_csv(os.path.join(output_path, f"annotated_clin_muts_{dt}.tsv"), sep="\t", index=False) 
    
    # merge annotated clinical variants with MegaMAF file, dedupe with preference to clin variants and add annotation columns to maf
    # also get stats for log file
    runner_logger.info("Merging and de-duping clinical and raw variants")
    concat_deduped, log_string = merge_clinical_variants_to_maf(maf_concat_local_path, anno_clin_muts)
    
    # save files 
    runner_logger.info("Saving merged and de-duped MAF")
    concat_deduped.to_csv(os.path.join(output_path, f"{maf_concat_local_path.split('.')[0]}_merged_clinical_variants_maf_{dt}.tsv"), sep="\t", index=False)
    
    runner_logger.info("Saving log file")
    with open(os.path.join(output_path, f"mci_clin_mutation_log_{dt}.txt"), "w") as log_file:
        log_file.write(f"Clinical Variant Merge Log - {dt}\n")
        log_file.write("="*50 + "\n")
        log_file.write(f"Input Clinical Variant File: {clin_var_local_path}\n")
        log_file.write(f"Input MAF: {maf_concat_local_path}\n")
        log_file.write(log_string)
        log_file.write(f"Number of clinical variants not annotated: {not_anno}\n")
    log_file.close()
    
    # upload to s3
    #upload output directory to S3
    runner_logger.info(f"Uploading outputs at {output_path} to {runner}")
    upload_folder_to_s3(
        local_folder=output_path,
        bucket=bucket,
        destination=runner,
        sub_folder=""
    )
    
    # remove output files after upload
    runner_logger.info(f"Removing outputs at {output_path}")
    shutil.rmtree(output_path)

if __name__ == "__main__":
    clin_anno_merge_flow(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])