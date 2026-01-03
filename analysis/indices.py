import ee

def add_indices(img):
    ndci = img.normalizedDifference(["B5", "B4"]).rename("NDCI")
    ndti = img.normalizedDifference(["B4", "B3"]).rename("NDTI")
    fai  = img.expression(
        "nir - (red + (swir - red) * ((nir_wl - red_wl)/(swir_wl - red_wl)))",
        {
            "nir": img.select("B8"),
            "red": img.select("B4"),
            "swir": img.select("B11"),
            "nir_wl": 842,
            "red_wl": 665,
            "swir_wl": 1610
        }
    ).rename("FAI")
    return img.addBands([ndci, ndti, fai])
