# data transforms for png_exports
# should scale raw min/max values from l3 files into range [0, 255]
png_export_transforms = {
    "chlor_a": "np.log10(data+1)/0.00519",
    "nflh":    "250*np.log10((0.59*(data*5)**.86)+1.025)/np.log10(2)"
}
