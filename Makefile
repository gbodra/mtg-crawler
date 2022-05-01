github:
	@echo "Committing changes to Github..."
	git add -A
	git commit -m "$m"
	git push