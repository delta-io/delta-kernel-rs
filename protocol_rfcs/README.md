# Delta Kernel Rust RFCs

Welcome to the Delta Kernel Rust RFC (Request for Comments) process!

The RFC process helps us:
- Collaboratively design and discuss substantial changes before implementation
- Explore alternatives and document decisions
- Ensure new features align with the project’s goals

## When should I write an RFC?

- For **substantial features** or **major design changes**
- When you want feedback on a big idea before starting implementation
- If a maintainer asks you to draft an RFC after an initial discussion

For **small bug fixes** or **minor enhancements**, you usually don’t need an RFC—just open a PR!

## How do I submit an RFC?

1. **Start a discussion:**  
   Open an issue or join a conversation in [Delta Lake Slack](https://go.delta.io/slack) (`#delta-kernel`) to share your idea and get early feedback.

2. **Write your RFC:**  
   Copy [`000-template.md`](./000-template.md) to a new file named `NNN-descriptive-title.md` (where NNN is the next available number).

3. **Open a Pull Request:**  
   Submit your RFC as a pull request to the `protocol_rfcs` directory. Link to any related issues or discussions.

4. **Collaborate:**  
   The community and maintainers will discuss, suggest changes, and iterate on the RFC.

5. **Decision:**  
   Once consensus is reached, the RFC will be marked as **Accepted** (or **Rejected**). Accepted RFCs can then be implemented via standard PRs.

## RFC Status

- **Draft:** Initial proposal, open for discussion
- **In Review:** Under active review by maintainers/community
- **Accepted:** Approved and ready for implementation
- **Rejected:** Not moving forward (with rationale)
- **Implemented:** Feature has been merged

## Tips for writing a great RFC

- Be concise but thorough—focus on motivation, design, and alternatives.
- Link to related issues, discussions, or prior art.
- Don’t worry about perfection—the process is collaborative!

---

We’re excited to hear your ideas and work together to make Delta Kernel Rust even better!
