package metadata

type Topic struct {
	Name            string `json:"name"`
	PartitionsCount int    `json:"partitions_count"`
}
