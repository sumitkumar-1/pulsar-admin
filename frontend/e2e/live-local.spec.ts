import { expect, test } from '@playwright/test';

test.describe('live local smoke @live', () => {
  test('creates a local environment and reaches the live explorer', async ({ page }) => {
    await page.goto('/environments');

    await expect(page.getByRole('heading', { name: 'Connected environments' })).toBeVisible();

    const addButton = page.getByRole('button', { name: 'Add Environment' });
    await addButton.click();

    await page.getByLabel(/Environment Id/i).fill(`local-e2e`);
    await page.getByLabel(/^Name$/i).fill('Local E2E');
    await page.getByLabel(/Kind/i).fill('local');
    await page.getByLabel(/Region/i).fill('local');
    await page.getByLabel(/Cluster Label/i).fill('standalone');
    await page.getByLabel(/Summary/i).fill('Local live verification environment');
    await page.getByLabel(/Broker Url/i).fill('pulsar://localhost:6650');
    await page.getByLabel(/Admin Url/i).fill('http://localhost:8081');
    await page.getByLabel(/Auth Mode/i).selectOption('none');
    await page.getByRole('button', { name: /Create Environment|Save Environment/i }).click();

    await expect(page.getByText('Local E2E')).toBeVisible();

    await page.getByRole('button', { name: 'Test + Sync' }).click();
    await expect(page.getByText(/SUCCESS|SYNCED|Metadata synced successfully/i)).toBeVisible({ timeout: 30_000 });

    await page.getByRole('link', { name: /Local E2E/i }).click();
    await expect(page.getByRole('heading', { name: 'Start with a namespace.' })).toBeVisible();
  });
});
