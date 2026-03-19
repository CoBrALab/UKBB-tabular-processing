for n in $(cut -f1 urls.tsv | tail -n +2); do
  wget -nd -O schema_${n}.tsv "https://biobank.ndph.ox.ac.uk/ukb/scdown.cgi?fmt=txt&id=${n}"
done
