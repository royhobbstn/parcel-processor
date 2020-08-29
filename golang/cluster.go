package main

import (
	"github.com/muesli/clusters"
	"github.com/muesli/kmeans"
	"encoding/json"
	"io/ioutil"
	"math"
	"os"
	"fmt"
)

// Centroid struct
type Centroid struct {
	ID  int32   `json:"id"`
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// ReturnID get the stupid ID
func (c Centroid) ReturnID() int32 {
	return c.ID
}

// Coordinates implemented Coordinates
func (c Centroid) Coordinates() clusters.Coordinates {
	return []float64{c.Lat, c.Lng}
}

// Distance implemented Distance
func (c Centroid) Distance(point clusters.Coordinates) float64 {
	coords := c.Coordinates()
	first := math.Pow(float64(coords[0]-point[0]), 2)
	second := math.Pow(float64(coords[1]-point[1]), 2)
	return math.Sqrt(first + second)
}

func main() {
    args := os.Args[1:]

	fmt.Println(args)
	
	inputNumClusters := args[0]
	inputFilename := args[1]
	outputFilename := args[2]

	fmt.Println("Number Of Clusters: " + inputNumClusters)
	fmt.Println("Input File: " + inputFilename)
	fmt.Println("Output File: " + outputFilename)

	centroids := []Centroid{}

	dat, err := ioutil.ReadFile(inputFilename)
	if err != nil {
		panic(err)
	}

	if err := json.Unmarshal(dat, &centroids); err != nil {
		panic(err)
	}

	var d clusters.Observations
	for _, x := range centroids {
		d = append(d, x)
	}

	// Partition the data points into X clusters
	km := kmeans.New()
	clusters, err := km.Partition(d, 50)

	clusterLookup := map[int32]int{}

	for i, c := range clusters {
		for _, val := range c.Observations {
			valID := val.(Centroid).ID
			clusterLookup[valID] = i
		}
	}

	mapB, err := json.Marshal(clusterLookup)
	if err != nil {
		panic(err)
	}
	
	if err := ioutil.WriteFile(outputFilename, mapB, 0644); err != nil {
		panic(err)
	}

}
