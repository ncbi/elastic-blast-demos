#!/bin/bash
# submit-and-wait-for-results.sh: End-to-end ElasticBLAST blast search with timeout.
#
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Fri Feb  5 13:52:36 EST 2021

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
set -euo pipefail

[ $# -gt 0 ] || { 
    echo "Usage: $0 <ELASTIC_BLAST_CONFIG_FILE> [timeout in minutes] [ElasticBLAST logfile name]"; 
    echo -e "\tdefault timeout is 500 minutes. ElasticBLAST search is DELETED after this time, even if not finished"
    echo -e "\tdefault logfile name is elastic-blast.log"
    exit 1; 
}

# All ElasticBLAST configuration settings are specified in the config file
CFG=${1}
timeout_minutes=${2:-500}
logfile=${3:-"elastic-blast.log"}
runsummary_output=${4:-elb-run-summary.json}
set +e
elb_results=`printenv ELB_RESULTS`
set -e
if [ -z "${elb_results}" ] ; then
    elb_results=`awk '/^results/ {print $NF}' $CFG` 
fi

DRY_RUN=''
#DRY_RUN=--dry-run     # uncomment for debugging
rm -f $logfile

get_num_cores() {
    retval=1
    if which parallel >&/dev/null; then
        retval=$(parallel --number-of-cores)
    elif [ -f /proc/cpuinfo ] ; then
        retval=$(grep -c '^proc' /proc/cpuinfo)
    elif which lscpu >& /dev/null; then
        retval=$(lscpu -p | grep -v '^#' | wc -l)
    elif [ `uname -s` == 'Darwin' ]; then
        retval=$(sysctl -n hw.ncpu)
    fi
    echo $retval
}
NTHREADS=$(get_num_cores)

cleanup_resources_on_error() {
    set +e
    elastic-blast delete --cfg $CFG --loglevel DEBUG --logfile $logfile $DRY_RUN
    exit 1;
}

TMP=`mktemp -t $(basename -s .sh $0)-XXXXXXX`
trap "cleanup_resources_on_error; /bin/rm -f $TMP" INT QUIT HUP KILL ALRM ERR

rm -fr *.out.gz
elastic-blast --version
elastic-blast submit --cfg $CFG --loglevel DEBUG --logfile $logfile $DRY_RUN

attempts=0
[ ! -z "$DRY_RUN" ] || sleep 10    # Should be enough for the BLAST k8s jobs to get started

while [ $attempts -lt $timeout_minutes ]; do
    elastic-blast status --cfg $CFG $DRY_RUN --logfile /dev/null | tee $TMP
    #set +e
    if grep '^Pending 0' $TMP && grep '^Running 0' $TMP; then
        break
    fi
    attempts=$(($attempts+1))
    sleep 60
    #set -e
done

elastic-blast run-summary --cfg $CFG --loglevel DEBUG --logfile $logfile -o $runsummary_output $DRY_RUN

# Get results
if ! grep -qi aws $CFG; then
    gsutil -qm cp ${elb_results}/*.out.gz .
else
    aws s3 cp ${elb_results}/ . --recursive --exclude '*' --include "*.out.gz" --exclude '*/*' --only-show-errors
fi

# Test results
if compgen -G "batch*.out.gz" >/dev/null ; then
    test $(du -a *.out.gz | sort -n | head -n 1 | cut -f 1) -gt 0
    find . -name "batch*.out.gz" -type f -print0 | xargs -0 -P $NTHREADS  -I{} gzip -t {}
else
    echo "ElasticBLAST produced no results"
fi
elastic-blast delete --cfg $CFG --loglevel DEBUG --logfile $logfile $DRY_RUN
