export default async function LinkedinBrightData(experience_level, time_range, keyword, country, job_type, remote, count){
	try{
		const data = JSON.stringify([
			{"location":"united States","keyword": keyword,"country":"US","time_range":time_range,"job_type":job_type,"experience_level":experience_level,"remote":remote,"company":"","location_radius":""},

		]);
		console.log(count)
	let linkedInfetch =await fetch(
		`https://api.brightdata.com/datasets/v3/scrape?dataset_id=gd_lpfll7v5hcqtkxl6l&include_errors=true&type=discover_new&discover_by=keyword&limit_per_input=${Number(count)}`,
		{
			method: "POST",        
			headers: {
				"Authorization": "Bearer ab23cac898b6dc350bae00969b913356b4c7145a1b96c0bf76b54c4f65978b14",
				"Content-Type": "application/json",
			},
			body: data,
		});
	let finalRes = await linkedInfetch.json();
	return finalRes;
	}catch(error){
		console.log(error);
	}
}
