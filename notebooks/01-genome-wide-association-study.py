#!pip3 install hail 

import os
os.environ['HAIL_HOME'] = '/home/cdsw/.local/lib/python3.6/site-packages/hail/backend'


# coding: utf-8

# ## Overview
# 
# This notebook is designed to provide a broad overview of Hail's functionality, with emphasis on the functionality to manipulate and query a genetic dataset. We walk through a genome-wide SNP association test, and demonstrate the need to control for confounding caused by population stratification.
# 

from pyspark.sql import SparkSession
import hail as hl
     
hl.init(tmp_dir="s3a://demo-aws-2/user/christopherroyles/data/hail/tmp")

# If the above cell ran without error, we're ready to go! 
# 
# Before using Hail, we import some standard Python libraries for use throughout the notebook.

from hail.plot import show
from pprint import pprint
hl.plot.output_notebook()

# ## Check for tutorial data or download if necessary
# 
# This cell downloads the necessary data if it isn't already present.

hl.utils.get_1kg('s3a://demo-aws-2/user/christopherroyles/data/hail/')

# ## Loading data from disk
# 
# Hail has its own internal data representation, called a MatrixTable. This is both an on-disk file format and a [Python object](https://hail.is/docs/devel/hail.MatrixTable.html#hail.MatrixTable). Here, we read a MatrixTable from disk.
# 
# This dataset was created by downsampling a public 1000 genomes VCF to about 50 MB.

hl.import_vcf('s3a://demo-aws-2/user/christopherroyles/data/hail/1kg.vcf.bgz').write('s3a://demo-aws-2/user/christopherroyles/data/hail/1kg.mt', overwrite=True)

mt = hl.read_matrix_table('s3a://demo-aws-2/user/christopherroyles/data/hail/1kg.mt')


# ### Getting to know our data
# 
# It's important to have easy ways to slice, dice, query, and summarize a dataset. Some of this functionality is demonstrated below.
# The [rows](https://hail.is/docs/0.2/hail.MatrixTable.html#hail.MatrixTable.rows) method can be used to get a table with all the row fields in our MatrixTable. 
# 
# We can use `rows` along with [select](https://hail.is/docs/0.2/hail.Table.html#hail.Table.select) to pull out 5 variants. The `select` method takes either a string refering to a field name in the table, or a Hail [Expression](https://hail.is/docs/0.2/hail.expr.Expression.html?#expression). Here, we leave the arguments blank to keep only the row key fields, `locus` and `alleles`.
# 
# Use the `show` method to display the variants.

# In[6]:


mt.rows().select().show(5)


# Alternatively:
# In[7]:


mt.row_key.show(5)


# Here is how to peek at the first few sample IDs:

# In[8]:


mt.s.show(5)


# To look at the first few genotype calls, we can use [entries](https://hail.is/docs/0.2/hail.MatrixTable.html#hail.MatrixTable.entries) along with `select` and `take`. The `take` method collects the first n rows into a list. Alternatively, we can use the `show` method, which prints the first n rows to the console in a table format. 
# 
# Try changing `take` to `show` in the cell below.

# In[9]:


mt.entry.take(5)


# ### Adding column fields
# 
# A Hail MatrixTable can have any number of row fields and column fields for storing data associated with each row and column. Annotations are usually a critical part of any genetic study. Column fields are where you'll store information about sample phenotypes, ancestry, sex, and covariates. Row fields can be used to store information like gene membership and functional impact for use in QC or analysis. 
# 
# In this tutorial, we demonstrate how to take a text file and use it to annotate the columns in a MatrixTable. 

# The file provided contains the sample ID, the population and "super-population" designations, the sample sex, and two simulated phenotypes (one binary, one discrete).

# This file can be imported into Hail with [import_table](https://hail.is/docs/0.2/methods/impex.html#hail.methods.import_table). This function produces a [Table](https://hail.is/docs/0.2/hail.Table.html#hail.Table) object. Think of this as a Pandas or R dataframe that isn't limited by the memory on your machine -- behind the scenes, it's distributed with Spark.

# In[10]:


table = (hl.import_table('s3a://demo-aws-2/user/christopherroyles/data/hail/1kg_annotations.txt', impute=True)
         .key_by('Sample'))


# A good way to peek at the structure of a `Table` is to look at its `schema`. 
# 
# 

# In[11]:


table.describe()


# To peek at the first few values, use the `show` method:

# In[12]:


table.show(width=100)


# Now we'll use this table to add sample annotations to our dataset, storing the annotations in column fields in our MatrixTable. First, we'll print the existing column schema:

# In[13]:


print(mt.col.dtype)


# We use the [annotate_cols](https://hail.is/docs/0.2/hail.MatrixTable.html#hail.MatrixTable.annotate_cols) method to join the table with the MatrixTable containing our dataset.

# In[14]:


mt = mt.annotate_cols(pheno = table[mt.s])


# In[15]:


mt.col.describe()


# ### Query functions and the Hail Expression Language
# 
# Hail has a number of useful query functions that can be used for gathering statistics on our dataset. These query functions take Hail Expressions as arguments.
# 
# We will start by looking at some statistics of the information in our table. The [aggregate](https://hail.is/docs/0.2/hail.Table.html#hail.Table.aggregate) method can be used to aggregate over rows of the table.

# `counter` is an aggregation function that counts the number of occurrences of each unique element. We can use this to pull out the population distribution by passing in a Hail Expression for the field that we want to count by.

# In[16]:


pprint(table.aggregate(hl.agg.counter(table.SuperPopulation)))


# `stats` is an aggregation function that produces some useful statistics about numeric collections. We can use this to see the distribution of the CaffeineConsumption phenotype.

# In[17]:


pprint(table.aggregate(hl.agg.stats(table.CaffeineConsumption)))


# However, these metrics aren't perfectly representative of the samples in our dataset. Here's why:

# In[18]:


table.count()


# In[19]:


mt.count_cols()


# Since there are fewer samples in our dataset than in the full thousand genomes cohort, we need to look at annotations on the dataset. We can use [aggregate_cols](https://hail.is/docs/0.2/hail.MatrixTable.html#hail.MatrixTable.aggregate_cols) to get the metrics for only the samples in our dataset.

# In[20]:


mt.aggregate_cols(hl.agg.counter(mt.pheno.SuperPopulation))


# In[21]:


pprint(mt.aggregate_cols(hl.agg.stats(mt.pheno.CaffeineConsumption)))


# The functionality demonstrated in the last few cells isn't anything especially new: it's certainly not difficult to ask these questions with Pandas or R dataframes, or even Unix tools like `awk`. But Hail can use the same interfaces and query language to analyze collections that are much larger, like the set of variants. 
# 
# Here we calculate the counts of each of the 12 possible unique SNPs (4 choices for the reference base * 3 choices for the alternate base). 
# 
# To do this, we need to get the alternate allele of each variant and then count the occurences of each unique ref/alt pair. This can be done with Hail's `counter` function.

# In[22]:


snp_counts = mt.aggregate_rows(hl.agg.counter(hl.Struct(ref=mt.alleles[0], alt=mt.alleles[1])))
pprint(snp_counts)


# We can list the counts in descending order using Python's Counter class.

# In[23]:


from collections import Counter
counts = Counter(snp_counts)
counts.most_common()


# It's nice to see that we can actually uncover something biological from this small dataset: we see that these frequencies come in pairs. C/T and G/A are actually the same mutation, just viewed from from opposite strands. Likewise, T/A and A/T are the same mutation on opposite strands. There's a 30x difference between the frequency of C/T and A/T SNPs. Why?

# The same Python, R, and Unix tools could do this work as well, but we're starting to hit a wall - the latest [gnomAD release](http://gnomad.broadinstitute.org/) publishes about 250 million variants, and that won't fit in memory on a single computer.
# 
# What about genotypes? Hail can query the collection of all genotypes in the dataset, and this is getting large even for our tiny dataset. Our 284 samples and 10,000 variants produce 10 million unique genotypes. The gnomAD dataset has about **5 trillion** unique genotypes.

# Hail plotting functions allow Hail fields as arguments, so we can pass in the DP field directly here. If the range and bins arguments are not set, this function will compute the range based on minimum and maximum values of the field and use the default 50 bins.

# In[24]:


p = hl.plot.histogram(mt.DP, range=(0,30), bins=30, title='DP Histogram', legend='DP')
show(p)


# ### Quality Control
# 
# QC is where analysts spend most of their time with sequencing datasets. QC is an iterative process, and is different for every project: there is no "push-button" solution for QC. Each time the Broad collects a new group of samples, it finds new batch effects. However, by practicing open science and discussing the QC process and decisions with others, we can establish a set of best practices as a community.

# QC is entirely based on the ability to understand the properties of a dataset. Hail attempts to make this easier by providing the [sample_qc](https://hail.is/docs/0.2/methods/genetics.html#hail.methods.sample_qc) function, which produces a set of useful metrics and stores them in a column field.

# In[25]:


mt.col.describe()


# In[26]:


mt = hl.sample_qc(mt)


# In[27]:


mt.col.describe()


# Plotting the QC metrics is a good place to start.

# In[28]:


p = hl.plot.histogram(mt.sample_qc.call_rate, range=(.88,1), legend='Call Rate')
show(p)


# In[29]:


p = hl.plot.histogram(mt.sample_qc.gq_stats.mean, range=(10,70), legend='Mean Sample GQ')
show(p)


# Often, these metrics are correlated.

# In[30]:


p = hl.plot.scatter(mt.sample_qc.dp_stats.mean, mt.sample_qc.call_rate, xlabel='Mean DP', ylabel='Call Rate')
show(p)


# Removing outliers from the dataset will generally improve association results. We can make arbitrary cutoffs and use them to filter:

# In[31]:


mt = mt.filter_cols((mt.sample_qc.dp_stats.mean >= 4) & (mt.sample_qc.call_rate >= 0.97))
print('After filter, %d/284 samples remain.' % mt.count_cols())


# Next is genotype QC. It's a good idea to filter out genotypes where the reads aren't where they should be: if we find a genotype called homozygous reference with >10% alternate reads, a genotype called homozygous alternate with >10% reference reads, or a genotype called heterozygote without a ref / alt balance near 1:1, it is likely to be an error.
# 
# In a low-depth dataset like 1KG, it is hard to detect bad genotypes using this metric, since a read ratio of 1 alt to 10 reference can easily be explained by binomial sampling. However, in a high-depth dataset, a read ratio of 10:100 is  a sure cause for concern!

# In[32]:


ab = mt.AD[1] / hl.sum(mt.AD)

filter_condition_ab = ((mt.GT.is_hom_ref() & (ab <= 0.1)) |
                        (mt.GT.is_het() & (ab >= 0.25) & (ab <= 0.75)) |
                        (mt.GT.is_hom_var() & (ab >= 0.9)))

fraction_filtered = mt.aggregate_entries(hl.agg.fraction(~filter_condition_ab))
print(f'Filtering {fraction_filtered * 100:.2f}% entries out of downstream analysis.')
mt = mt.filter_entries(filter_condition_ab)


# In[ ]:





# Variant QC is a bit more of the same: we can use the [variant_qc](https://hail.is/docs/0.2/methods/genetics.html?highlight=variant%20qc#hail.methods.variant_qc) function to produce a variety of useful statistics, plot them, and filter.

# In[33]:


mt = hl.variant_qc(mt)


# In[34]:


mt.row.describe()


# These statistics actually look pretty good: we don't need to filter this dataset. Most datasets require thoughtful quality control, though. The [filter_rows](https://hail.is/docs/0.2/hail.MatrixTable.html#hail.MatrixTable.filter_rows) method can help!

# ### Let's do a GWAS!
# 
# First, we need to restrict to variants that are : 
# 
#  - common (we'll use a cutoff of 1%)
#  - not so far from [Hardy-Weinberg equilibrium](https://en.wikipedia.org/wiki/Hardy%E2%80%93Weinberg_principle) as to suggest sequencing error
# 

# In[35]:


mt = mt.filter_rows(mt.variant_qc.AF[1] > 0.01)


# In[36]:


mt = mt.filter_rows(mt.variant_qc.p_value_hwe > 1e-6)


# In[37]:


print('Samples: %d  Variants: %d' % (mt.count_cols(), mt.count_rows()))


# These filters removed about 15% of sites (we started with a bit over 10,000). This is _NOT_ representative of most sequencing datasets! We have already downsampled the full thousand genomes dataset to include more common variants than we'd expect by chance.
# 
# In Hail, the association tests accept column fields for the sample phenotype and covariates. Since we've already got our phenotype of interest (caffeine consumption) in the dataset, we are good to go:

# In[38]:


gwas = hl.linear_regression_rows(y=mt.pheno.CaffeineConsumption, 
                                 x=mt.GT.n_alt_alleles(), 
                                 covariates=[1.0])
gwas.row.describe()


# Looking at the bottom of the above printout, you can see the linear regression adds new row fields for the beta, standard error, t-statistic, and p-value.
# 
# Hail makes it easy to visualize results! Let's make a [Manhattan plot](https://en.wikipedia.org/wiki/Manhattan_plot):

# In[39]:


p = hl.plot.manhattan(gwas.p_value)
show(p)


# This doesn't look like much of a skyline. Let's check whether our GWAS was well controlled using a [Q-Q (quantile-quantile) plot](https://en.wikipedia.org/wiki/Q–Q_plot).

# In[40]:


p = hl.plot.qq(gwas.p_value)
show(p)


# ### Confounded!
# 
# The observed p-values drift away from the expectation immediately. Either every SNP in our dataset is causally linked to caffeine consumption (unlikely), or there's a confounder.
# 
# We didn't tell you, but sample ancestry was actually used to simulate this phenotype. This leads to a [stratified](https://en.wikipedia.org/wiki/Population_stratification) distribution of the phenotype. The solution is to include ancestry as a covariate in our regression. 
# 
# The [linear_regression_rows](https://hail.is/docs/0.2/methods/stats.html#hail.methods.linear_regression_rows) function can also take column fields to use as covariates. We already annotated our samples with reported ancestry, but it is good to be skeptical of these labels due to human error. Genomes don't have that problem! Instead of using reported ancestry, we will use genetic ancestry by including computed principal components in our model.
# 
# The [pca](https://hail.is/docs/0.2/methods/stats.html#hail.methods.pca) function produces eigenvalues as a list and sample PCs as a Table, and can also produce variant loadings when asked. The [hwe_normalized_pca](https://hail.is/docs/0.2/methods/genetics.html#hail.methods.hwe_normalized_pca) function does the same, using HWE-normalized genotypes for the PCA.

# In[41]:


eigenvalues, pcs, _ = hl.hwe_normalized_pca(mt.GT)


# In[42]:


pprint(eigenvalues)


# In[43]:


pcs.show(5, width=100)


# Now that we've got principal components per sample, we may as well plot them! Human history exerts a strong effect in genetic datasets. Even with a 50MB sequencing dataset, we can recover the major human populations.

# In[44]:


mt = mt.annotate_cols(scores = pcs[mt.s].scores)


# In[45]:


p = hl.plot.scatter(mt.scores[0], 
                    mt.scores[1],
                    label=mt.pheno.SuperPopulation,
                    title='PCA', xlabel='PC1', ylabel='PC2')
show(p)


# Now we can rerun our linear regression, controlling for sample sex and the first few principal components. We'll do this with input variable the number of alternate alleles as before, and again with input variable the genotype dosage derived from the PL field.

# In[46]:


gwas = hl.linear_regression_rows(
    y=mt.pheno.CaffeineConsumption, 
    x=mt.GT.n_alt_alleles(),
    covariates=[1.0, mt.pheno.isFemale, mt.scores[0], mt.scores[1], mt.scores[2]])


# We'll first make a Q-Q plot to assess inflation...

# In[47]:


p = hl.plot.qq(gwas.p_value)
show(p)


# That's more like it! This shape is indicative of a well-controlled (but not especially well-powered) study. And now for the Manhattan plot:

# In[48]:


p = hl.plot.manhattan(gwas.p_value)
show(p)


# We have found a caffeine consumption locus! Now simply apply Hail's Nature paper function to publish the result. 
# 
# Just kidding, that function won't land until Hail 1.0!

# ### Rare variant analysis
# 
# Here we'll demonstrate how one can use the expression language to group and count by any arbitrary properties in row and column fields. Hail also implements the sequence kernel association test (SKAT).
# 

# In[49]:


entries = mt.entries()
results = (entries.group_by(pop = entries.pheno.SuperPopulation, chromosome = entries.locus.contig)
      .aggregate(n_het = hl.agg.count_where(entries.GT.is_het())))


# In[50]:


results.show()


# What if we want to group by minor allele frequency bin and hair color, and calculate the mean GQ?

# In[51]:


entries = entries.annotate(maf_bin = hl.cond(entries.info.AF[0]<0.01, "< 1%", 
                             hl.cond(entries.info.AF[0]<0.05, "1%-5%", ">5%")))

results2 = (entries.group_by(af_bin = entries.maf_bin, purple_hair = entries.pheno.PurpleHair)
      .aggregate(mean_gq = hl.agg.stats(entries.GQ).mean, 
                 mean_dp = hl.agg.stats(entries.DP).mean))


# In[52]:


results2.show()


# We've shown that it's easy to aggregate by a couple of arbitrary statistics. This specific examples may not provide especially useful pieces of information, but this same pattern can be used to detect effects of rare variation:
# 
#  - Count the number of heterozygous genotypes per gene by functional category (synonymous, missense, or loss-of-function) to estimate per-gene functional constraint
#  - Count the number of singleton loss-of-function mutations per gene in cases and controls to detect genes involved in disease

# ### Epilogue
# 
# Congrats! You've reached the end of the first tutorial. To learn more about Hail's API and functionality, take a look at the other tutorials. You can check out the [Python API](https://hail.is/docs/0.2/api.html#python-api) for documentation on additional Hail functions. If you use Hail for your own science, we'd love to hear from you on [Zulip chat](https://hail.zulipchat.com) or the [discussion forum](http://discuss.hail.is).
# 
# For reference, here's the full workflow to all tutorial endpoints combined into one cell.

# In[53]:


table = hl.import_table('s3a://demo-aws-2/user/christopherroyles/data/hail/1kg_annotations.txt', impute=True).key_by('Sample')

mt = hl.read_matrix_table('s3a://demo-aws-2/user/christopherroyles/data/hail/1kg.mt')
mt = mt.annotate_cols(pheno = table[mt.s])
mt = hl.sample_qc(mt)
mt = mt.filter_cols((mt.sample_qc.dp_stats.mean >= 4) & (mt.sample_qc.call_rate >= 0.97))
ab = mt.AD[1] / hl.sum(mt.AD)
filter_condition_ab = ((mt.GT.is_hom_ref() & (ab <= 0.1)) |
                        (mt.GT.is_het() & (ab >= 0.25) & (ab <= 0.75)) |
                        (mt.GT.is_hom_var() & (ab >= 0.9)))
mt = mt.filter_entries(filter_condition_ab)
mt = hl.variant_qc(mt)
mt = mt.filter_rows(mt.variant_qc.AF[1] > 0.01)

eigenvalues, pcs, _ = hl.hwe_normalized_pca(mt.GT)

mt = mt.annotate_cols(scores = pcs[mt.s].scores)
gwas = hl.linear_regression_rows(
    y=mt.pheno.CaffeineConsumption, 
    x=mt.GT.n_alt_alleles(),
    covariates=[1.0, mt.pheno.isFemale, mt.scores[0], mt.scores[1], mt.scores[2]])


# In[ ]:




