console.log("loaded");

const width = window.innerWidth;
const height = window.innerHeight;

const svg = d3
  .select("#main_canvas")
  .attr("viewBox", [0, 0, width, height])
  .attr("preserveAspectRatio", "xMidYMid meet");

d3.json("network.json").then((data) => {
  const linkScale = d3
    .scaleLinear()
    .domain(d3.extent(data.links, (d) => d.weight))
    .range([1, 5]);

  const opacityScale = d3
    .scaleLinear()
    .domain(d3.extent(data.links, (d) => d.weight))
    .range([0.3, 1]);

  const radiusScale = d3
    .scaleSqrt()
    .domain(d3.extent(data.nodes, (d) => d.occurrence))
    .range([5, 20]);

  const simulation = d3
    .forceSimulation(data.nodes)
    .force(
      "link",
      d3
        .forceLink(data.links)
        .id((d) => d.id)
        .distance(100),
    )
    .force("charge", d3.forceManyBody().strength(-200))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .on("end", () => {
      simulation.stop();
    });

  const link = svg
    .append("g")
    .attr("stroke", "#999")
    .attr("stroke-opacity", (d) => opacityScale(d.weight))
    .selectAll("line")
    .data(data.links)
    .join("line")
    .attr("stroke-width", (d) => linkScale(d.weight));

  const node = svg
    .append("g")
    .attr("stroke", "#fff")
    .attr("stroke-width", 1.5)
    .selectAll("circle")
    .data(data.nodes)
    .join("circle")
    .attr("r", (d) => radiusScale(d.occurrence))
    .attr("fill", "#69b3a2")
    .append("title")
    .text((d) => d.name);

  simulation.on("tick", () => {
    link
      .attr("x1", (d) => d.source.x)
      .attr("y1", (d) => d.source.y)
      .attr("x2", (d) => d.target.x)
      .attr("y2", (d) => d.target.y);

    svg
      .selectAll("circle")
      .attr("cx", (d) => d.x)
      .attr("cy", (d) => d.y);
  });
});
