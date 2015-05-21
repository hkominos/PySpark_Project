import pysam
samfile = pysam.AlignmentFile("/home/henrik/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam", "rb")

print "###################"
print samfile.references
print len(list(samfile.fetch()))
for read in samfile.fetch(region="20"):
	isize=alignment.tlen   #This is column 9, the size
     print read

samfile.close()
